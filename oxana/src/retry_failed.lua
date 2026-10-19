-- Redis script errors do not roll back writes. Validate key types before mutation,
-- and retain the processing claim until the retry destination has been written.
local types = {'hash', 'zset', 'list'}
for i, expected in ipairs(types) do
    local actual = redis.call('type', KEYS[i]).ok
    if actual ~= 'none' and actual ~= expected then
        return redis.error_reply('WRONGTYPE retry transition key is not a ' .. expected)
    end
end
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
redis.call('zadd', KEYS[2], ARGV[3], ARGV[1])
redis.call('lrem', KEYS[3], 1, ARGV[1])
return nil
