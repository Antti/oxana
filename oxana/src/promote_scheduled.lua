-- Compare the snapshot before moving: another scheduler may have already run
-- this job and scheduled its next retry while we were reading the payload.
local types = {'hash', 'zset', 'list'}
for i, expected in ipairs(types) do
    local actual = redis.call('type', KEYS[i]).ok
    if actual ~= 'none' and actual ~= expected then
        return redis.error_reply('WRONGTYPE scheduled transition key is not a ' .. expected)
    end
end
local score = redis.call('zscore', KEYS[2], ARGV[1])
if not score or tonumber(score) ~= tonumber(ARGV[3]) then return 0 end
local payload = redis.call('hget', KEYS[1], ARGV[1])
local expected = ARGV[2] ~= '' and ARGV[2] or false
if payload ~= expected then return 0 end
if payload then redis.call('lpush', KEYS[3], ARGV[1]) end
redis.call('zrem', KEYS[2], ARGV[1])
return payload and 1 or 0
