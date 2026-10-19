-- Script errors do not roll back earlier writes, so validate both keys before
-- deleting payloads. Repeating a committed acknowledgment is harmless.
local types = {'hash', 'list'}
for i, expected in ipairs(types) do
    local actual = redis.call('type', KEYS[i]).ok
    if actual ~= 'none' and actual ~= expected then
        return redis.error_reply('WRONGTYPE acknowledgment key is not a ' .. expected)
    end
end
for _, id in ipairs(ARGV) do
    redis.call('hdel', KEYS[1], id)
    redis.call('lrem', KEYS[2], 1, id)
end
return nil
