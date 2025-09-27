--[[
Description:
  Registers a processor with Redis. ZADD the processor's name into a set of active processors and sets its pool
  size to ZCARD(heartbeats) + 1.
  Also sets the steady state of the processor to false if it doesn't exist.
  Returns the steady state along with pool size.

KEYS array:
  KEYS[1] - The base key prefix (e.g., "pipeline:vertex")

ARGV array:
  ARGV[1] - The name of the processor (e.g., "processor-alpha-1")

Returns:
  ZCARD(heartbeats) which is the value stored at poolsize_key for this processor(heartbeats includes this processor)
]]--

-- Construct the full key names from the provided prefix
local heartbeat_key = KEYS[1] .. ":heartbeats"
local poolsize_key = KEYS[1] .. ":poolsize"
local pre_max_filled_key = KEYS[1] .. ":prev_max_filled"

-- Get the current timestamp for the heartbeat
local current_timestamp = redis.call('TIME')[1]

-- Add the processor to the heartbeats set with current timestamp
redis.call('ZADD', heartbeat_key, current_timestamp, ARGV[1])

-- Calculate the new pool size: current heartbeat count after adding this processor
local heartbeat_count = redis.call('ZCARD', heartbeat_key)
local new_pool_size = heartbeat_count

-- Add the processor to the poolsize set with the calculated pool size
redis.call('ZADD', poolsize_key, new_pool_size, ARGV[1])

-- Check if there exists a prev_max_filled for the processor
-- return that if it exists, otherwise return -1
local stored_prev_max_filled = redis.call('HGET', pre_max_filled_key, ARGV[1])
local prev_max_filled = -1
-- If the key doesn't exist, redis' nil gets converted to false in lua
-- https://redis.io/docs/latest/develop/programmability/lua-api/#resp2-to-lua-type-conversion
if stored_prev_max_filled ~= false then
    prev_max_filled = stored_prev_max_filled
end

-- Return the pool size that was stored for this processor
return {new_pool_size, prev_max_filled}
