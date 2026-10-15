-- Never pop a claim until its destination is durable. The head/payload checks
-- also make competing recovery processes harmless.
local types = {'hash', 'list', 'list'}
for i, expected in ipairs(types) do
    local actual = redis.call('type', KEYS[i]).ok
    if actual ~= 'none' and actual ~= expected then
        return redis.error_reply('WRONGTYPE recovery transition key is not a ' .. expected)
    end
end
if redis.call('lindex', KEYS[2], 0) ~= ARGV[1] then return 0 end
local payload = redis.call('hget', KEYS[1], ARGV[1])
local expected = ARGV[2] ~= '' and ARGV[2] or false
if payload ~= expected then return 0 end
if payload then
    if ARGV[3] == '1' then
        redis.call('lpush', KEYS[3], ARGV[1])
    else
        redis.call('hdel', KEYS[1], ARGV[1])
    end
end
redis.call('lpop', KEYS[2])
return 1
