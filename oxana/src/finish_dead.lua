-- Save the terminal payload before releasing either the active payload or claim.
local types = {'hash', 'list', 'list'}
for i, expected in ipairs(types) do
    local actual = redis.call('type', KEYS[i]).ok
    if actual ~= 'none' and actual ~= expected then
        return redis.error_reply('WRONGTYPE dead transition key is not a ' .. expected)
    end
end
redis.call('lpush', KEYS[2], ARGV[2])
redis.call('hdel', KEYS[1], ARGV[1])
redis.call('lrem', KEYS[3], 1, ARGV[1])
local limit = tonumber(ARGV[3])
if limit > 0 then redis.call('ltrim', KEYS[2], 0, limit - 1) end
return nil
