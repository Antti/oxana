-- KEYS: jobs, schedule, retry, followed by old and new queue keys.
-- ARGV: destination, snapshot count,
-- then (id, comparison mode, expected bytes, old queue key index) snapshots,
-- then (id, payload, new queue key index, conflict mode, score) inputs.
-- Comparison modes: 0 = absent, 1 = exact payload,
-- 2 = JSON suffix starting at root queue, 3 = JSON prefix ending at root queue.
-- Return (changed snapshots, outcomes). A stale snapshot causes no writes.
-- Outcomes: 0 = skipped, 1 = enqueued, 2 = replaced.
local types = {}
local function require_type(key, expected)
    if types[key] then return types[key] == expected end
    types[key] = expected
    local actual = redis.call('type', key).ok
    return actual == 'none' or actual == expected
end
local function wrong_type()
    return redis.error_reply('WRONGTYPE enqueue transition key has an incompatible type')
end
if not require_type(KEYS[1], 'hash') then return wrong_type() end

-- Rust extracts previous queue names. Validate the original JSON prefix/suffix
-- (or full payload for other layouts) without parsing JSON on Redis's event loop.
local snapshot_count = tonumber(ARGV[2])
local present = {}
local queue_keys = {}
local changed = {}
for i = 3, 2 + snapshot_count * 4, 4 do
    local id = ARGV[i]
    local payload = redis.call('hget', KEYS[1], id)
    local expected = ARGV[i + 1] ~= '0' and ARGV[i + 2] or false
    local actual = payload
    if payload and ARGV[i + 1] == '2' then
        actual = string.sub(payload, -#expected)
    elseif payload and ARGV[i + 1] == '3' then
        actual = string.sub(payload, 1, #expected)
    end
    if actual ~= expected then
        table.insert(changed, {id, payload})
    end
    present[id] = payload ~= false
    queue_keys[id] = KEYS[tonumber(ARGV[i + 3])]
end
if #changed > 0 then return {changed, {}} end

-- Plan in input order. Skip reads only existence, and preceding writes supply
-- the old queue for subsequent replacements of the same ID in this chunk.
local writes, outcomes = {}, {}
for i = 3 + snapshot_count * 4, #ARGV, 5 do
    local id, queue, conflict = ARGV[i], KEYS[tonumber(ARGV[i + 2])], ARGV[i + 3]
    local exists = present[id]
    if exists == nil and conflict ~= 'default' then
        exists = redis.call('hexists', KEYS[1], id) == 1
        present[id] = exists
    end
    if conflict == 'skip' and exists then
        table.insert(outcomes, 0)
    else
        local old_queue = false
        local replacing = conflict == 'replace' and exists
        if replacing then
            old_queue = queue_keys[id]
        end
        table.insert(writes, {arg = i, old_queue = old_queue, new_queue = queue, replacing = replacing})
        table.insert(outcomes, replacing and 2 or 1)
        present[id], queue_keys[id] = true, queue
    end
end

-- Redis script errors do not roll back writes. Preflight every destination and
-- cleanup key, including aliases, before committing any job in the chunk.
local scheduled = ARGV[1] == 'schedule'
for _, write in ipairs(writes) do
    if scheduled and not require_type(KEYS[2], 'zset') then return wrong_type() end
    if not scheduled and not require_type(write.new_queue, 'list') then return wrong_type() end
    if write.replacing then
        if not require_type(KEYS[2], 'zset')
            or not require_type(KEYS[3], 'zset')
            or not require_type(write.old_queue, 'list')
            or not require_type(write.new_queue, 'list') then
            return wrong_type()
        end
    end
end

for _, write in ipairs(writes) do
    local i = write.arg
    local id = ARGV[i]
    if write.replacing then
        redis.call('zrem', KEYS[2], id)
        redis.call('zrem', KEYS[3], id)
        redis.call('lrem', write.old_queue, 0, id)
        if write.old_queue ~= write.new_queue then
            redis.call('lrem', write.new_queue, 0, id)
        end
    end
    -- Preserve the original JSON bytes, including integers outside Lua's range.
    redis.call('hset', KEYS[1], id, ARGV[i + 1])
    if scheduled then
        redis.call('zadd', KEYS[2], ARGV[i + 4], id)
    else
        redis.call('lpush', write.new_queue, id)
    end
end
return {{}, outcomes}
