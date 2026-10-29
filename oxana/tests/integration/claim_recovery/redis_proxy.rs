use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use deadpool_redis::redis::{self, ConnectionAddr};
use testresult::TestResult;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::{JoinHandle, JoinSet};

/// Forwards RESP2 traffic to real Redis, with controlled response failures.
/// In particular, a failed LMOVE response happens *after* Redis commits it.
pub struct RedisProxy {
    pub pool: deadpool_redis::Pool,
    fault: Arc<FaultState>,
    task: JoinHandle<()>,
}

struct FaultState {
    prefix: Vec<String>,
    disconnect: bool,
    repeat: bool,
    reject_before: bool,
    injected: AtomicBool,
    faults: AtomicUsize,
    matching_requests: AtomicUsize,
    claims: AtomicUsize,
    request_bytes: AtomicUsize,
    response_bytes: AtomicUsize,
    outage: AtomicBool,
    fail_reads: AtomicBool,
    pause_reads: AtomicBool,
    paused: Notify,
    resume: Notify,
}

impl RedisProxy {
    pub async fn observe() -> TestResult<Self> {
        let proxy = Self::start_with_fault(Vec::new(), false, false, false).await?;
        proxy.fault.fail_reads.store(false, Ordering::SeqCst);
        Ok(proxy)
    }

    pub async fn start(command_prefix: Vec<String>, disconnect: bool) -> TestResult<Self> {
        Self::start_with_fault(command_prefix, disconnect, false, false).await
    }

    pub async fn reject_once(command_prefix: Vec<String>) -> TestResult<Self> {
        Self::start_with_fault(command_prefix, false, false, true).await
    }

    pub async fn failing_reads(command_prefix: Vec<String>) -> TestResult<Self> {
        Self::start_with_fault(command_prefix, false, true, false).await
    }

    /// Hold one matching response after Redis has executed the command.
    pub async fn pause_once(command_prefix: Vec<String>) -> TestResult<Self> {
        let proxy = Self::start_with_fault(command_prefix, false, false, false).await?;
        proxy.fault.fail_reads.store(false, Ordering::SeqCst);
        proxy.fault.pause_reads.store(true, Ordering::SeqCst);
        Ok(proxy)
    }

    pub async fn wait_until_paused(&self) {
        self.fault.paused.notified().await;
    }

    pub fn resume(&self) {
        self.fault.resume.notify_one();
    }

    pub fn injected(&self) -> bool {
        self.fault.injected.load(Ordering::SeqCst)
    }

    pub fn fault_count(&self) -> usize {
        self.fault.faults.load(Ordering::SeqCst)
    }

    pub fn matching_request_count(&self) -> usize {
        self.fault.matching_requests.load(Ordering::SeqCst)
    }

    pub fn claim_count(&self) -> usize {
        self.fault.claims.load(Ordering::SeqCst)
    }

    /// Serialized client requests and Redis replies observed by this proxy.
    pub fn traffic_bytes(&self) -> (usize, usize) {
        (
            self.fault.request_bytes.load(Ordering::SeqCst),
            self.fault.response_bytes.load(Ordering::SeqCst),
        )
    }

    pub fn set_outage(&self, outage: bool) {
        self.fault.outage.store(outage, Ordering::SeqCst);
    }

    pub fn restore_reads(&self) {
        self.fault.fail_reads.store(false, Ordering::SeqCst);
    }

    async fn start_with_fault(
        command_prefix: Vec<String>,
        disconnect: bool,
        repeat: bool,
        reject_before: bool,
    ) -> TestResult<Self> {
        let client = redis::Client::open(std::env::var("REDIS_URL")?)?;
        let info = client.get_connection_info().clone();
        let ConnectionAddr::Tcp(host, port) = info.addr().clone() else {
            return Err("Redis fault tests require a TCP Redis URL".into());
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let config = deadpool_redis::Config::from_connection_info(info.set_addr(
            ConnectionAddr::Tcp(address.ip().to_string(), address.port()),
        ));
        let pool = config.create_pool(Some(deadpool_redis::Runtime::Tokio1))?;
        let fault = Arc::new(FaultState {
            prefix: command_prefix,
            disconnect,
            repeat,
            reject_before,
            injected: AtomicBool::new(false),
            faults: AtomicUsize::new(0),
            matching_requests: AtomicUsize::new(0),
            claims: AtomicUsize::new(0),
            request_bytes: AtomicUsize::new(0),
            response_bytes: AtomicUsize::new(0),
            outage: AtomicBool::new(false),
            fail_reads: AtomicBool::new(true),
            pause_reads: AtomicBool::new(false),
            paused: Notify::new(),
            resume: Notify::new(),
        });
        let task_fault = Arc::clone(&fault);
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let host = host.clone();
                        let fault = Arc::clone(&task_fault);
                        connections.spawn(async move {
                            let result = forward(socket, &host, port, fault).await;
                            if let Err(error) = result {
                                assert!(matches!(error.kind(), io::ErrorKind::UnexpectedEof | io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe), "{error}");
                            }
                        });
                    }
                    Some(result) = connections.join_next() => result.unwrap(),
                }
            }
        });
        Ok(Self { pool, fault, task })
    }
}

impl Drop for RedisProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn forward(
    socket: TcpStream,
    host: &str,
    port: u16,
    fault: Arc<FaultState>,
) -> io::Result<()> {
    let upstream = TcpStream::connect((host, port)).await?;
    upstream.set_nodelay(true)?;
    socket.set_nodelay(true)?;
    let mut upstream = BufReader::new(upstream);
    let mut socket = BufReader::new(socket);
    loop {
        let request = read_frame(&mut socket).await?;
        fault
            .request_bytes
            .fetch_add(request.len(), Ordering::SeqCst);
        if fault.outage.load(Ordering::SeqCst) {
            return Ok(());
        }
        let command: Vec<String> =
            redis::from_redis_value(redis::parse_redis_value(&request).map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
        if command.starts_with(&fault.prefix) {
            fault.matching_requests.fetch_add(1, Ordering::SeqCst);
        }
        if fault.reject_before
            && command.starts_with(&fault.prefix)
            && !fault.injected.swap(true, Ordering::SeqCst)
        {
            fault.faults.fetch_add(1, Ordering::SeqCst);
            socket
                .get_mut()
                .write_all(b"-ERR injected write rejection\r\n")
                .await?;
            continue;
        }
        upstream.get_mut().write_all(&request).await?;
        let response = read_frame(&mut upstream).await?;
        fault
            .response_bytes
            .fetch_add(response.len(), Ordering::SeqCst);

        if fault.pause_reads.load(Ordering::SeqCst)
            && command.starts_with(&fault.prefix)
            && !fault.injected.swap(true, Ordering::SeqCst)
        {
            fault.paused.notify_one();
            fault.resume.notified().await;
        }

        if command.first().is_some_and(|name| name == "LMOVE")
            && response.starts_with(b"$")
            && response != b"$-1\r\n"
        {
            fault.claims.fetch_add(1, Ordering::SeqCst);
        }
        if !fault.reject_before
            && command.starts_with(&fault.prefix)
            && !response.starts_with(b"-")
            && (command.first().is_some_and(|name| name == "EVALSHA") || response != b"$-1\r\n")
            && fault.fail_reads.load(Ordering::SeqCst)
            && (fault.repeat || !fault.injected.swap(true, Ordering::SeqCst))
        {
            fault.injected.store(true, Ordering::SeqCst);
            fault.faults.fetch_add(1, Ordering::SeqCst);
            if fault.disconnect {
                return Ok(());
            }
            let response: &[u8] = if fault.repeat {
                b"-TRYAGAIN injected payload read failure\r\n"
            } else {
                b"-ERR injected claim failure\r\n"
            };
            socket.get_mut().write_all(response).await?;
        } else {
            socket.get_mut().write_all(&response).await?;
        }
    }
}

async fn read_frame(stream: &mut BufReader<TcpStream>) -> io::Result<Vec<u8>> {
    let mut frame = Vec::new();
    let mut remaining = 1;
    while remaining > 0 {
        let start = frame.len();
        if stream.read_until(b'\n', &mut frame).await? == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        remaining -= 1;
        let prefix = *frame.get(start).ok_or(io::ErrorKind::UnexpectedEof)?;
        match prefix {
            b'*' | b'$' => {
                let digits = frame
                    .get(start + 1..frame.len().saturating_sub(2))
                    .ok_or(io::ErrorKind::InvalidData)?;
                let length: i64 = std::str::from_utf8(digits)
                    .map_err(io::Error::other)?
                    .parse()
                    .map_err(io::Error::other)?;
                if prefix == b'*' {
                    remaining += length.max(0);
                } else if length >= 0 {
                    let start = frame.len();
                    frame.resize(
                        start + usize::try_from(length).map_err(io::Error::other)? + 2,
                        0,
                    );
                    let body = frame.get_mut(start..).ok_or(io::ErrorKind::UnexpectedEof)?;
                    stream.read_exact(body).await?;
                }
            }
            b'+' | b'-' | b':' => {}
            other => return Err(io::Error::other(format!("Unexpected RESP2 prefix {other}"))),
        }
    }
    Ok(frame)
}
