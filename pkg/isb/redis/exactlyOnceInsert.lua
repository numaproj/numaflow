-- Lua script inserts an object into the stream only if the element has not been previously written (exactly-once-semantics).
-- The uniqueness of the object is defined by the offset which is provided as an argument to the lua script. It will
-- return the inserted offset (or the storedOffset during replay). This offset is used to track watermark also.
-- KEYS: hash
--       stream
-- ARGS: prev-offset
--       field (header)
--       value (body)
-- RET:  offset

local hash = KEYS[1]
local offset = ARGV[1]

local stream = KEYS[2]
local field = ARGV[2]
local value = ARGV[3]

local minid = ARGV[4]
-- optionals
local _EXPIRY = ARGV[5] or 600

local storedOffset = redis.call('HGET', hash, offset)
if storedOffset == false then
    local insertedOffset = redis.call('XADD', stream, 'MINID', '~', minid, '*', field, value)
    if insertedOffset ~= false then
        redis.call('HSET', hash, offset, insertedOffset)
        redis.call('EXPIRE', hash, _EXPIRY)
        return insertedOffset
    else
        return redis.error_reply('error in XADD' .. insertedOffset)
    end
end
return storedOffset

