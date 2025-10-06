--[[
Description:
  Registers a processor with Redis. ZADD the processor's name into a set of active processors and sets its pool
  size to ZCARD(heartbeats) + 1.
  Also clean up stale entries in prev_max_filled and prev_max_filled_ttl based on STALE_AGE.
  Returns the pool size of the processor and the prev_max_filled for it if it exists.

KEYS array:
  KEYS[1] - The base key prefix (e.g., "pipeline:vertex")

ARGV array:
  ARGV[1] - The name of the processor (e.g., "processor-alpha-1")
  ARGV[2] - The stale age in seconds

Returns:
  ZCARD(heartbeats) which is the value stored at poolsize_key for this processor(heartbeats includes this processor)
]] --

-- Define the stale age in seconds
local STALE_AGE = tonumber(ARGV[2])

-- Construct the full key names from the provided prefix
local heartbeat_key = KEYS[1] .. ":heartbeats"
local poolsize_key = KEYS[1] .. ":poolsize"
local pre_max_filled_key = KEYS[1] .. ":prev_max_filled"
local prev_max_filled_ttl_key = KEYS[1] .. ":prev_max_filled_ttl"

-- Get the current timestamp for the heartbeat
local current_timestamp = redis.call('TIME')[1]

-- Step 1: Register the new processor, create heartbeat for it and update the pool size
-- Add the processor to the heartbeats set with current timestamp
redis.call('ZADD', heartbeat_key, current_timestamp, ARGV[1])

-- Calculate the new pool size: current heartbeat count after adding this processor
local heartbeat_count = redis.call('ZCARD', heartbeat_key)
local new_pool_size = heartbeat_count

-- Add the processor to the poolsize set with the calculated pool size
redis.call('ZADD', poolsize_key, new_pool_size, ARGV[1])


-- Step 2: Clean up prev_max_filled hash set based on TTL (STALE_AGE)
-- A processor's prev_max_filled is stale if it was deregistered more than STALE_AGE seconds ago
local stale_threshold = current_timestamp - STALE_AGE

-- Find all members with a score (timestamp) older than the threshold
local stale_members = redis.call('ZRANGEBYSCORE', prev_max_filled_ttl_key, '-inf', '(' .. stale_threshold)

-- If any stale members were found, remove them from both sets
if #stale_members > 0 then
    -- The unpack function passes the table elements as individual arguments to ZREM
    redis.call('ZREM', prev_max_filled_ttl_key, unpack(stale_members))
    redis.call('HDEL', pre_max_filled_key, unpack(stale_members))
end

-- Step 3: If this processor was recently deregistered, get the prev_max_filled for it
-- Check if there exists a prev_max_filled for the processor
-- return that if it exists, otherwise return -1
local stored_prev_max_filled = redis.call('HGET', pre_max_filled_key, ARGV[1])
local prev_max_filled = 0
-- If the key doesn't exist, redis' nil gets converted to false in lua
-- https://redis.io/docs/latest/develop/programmability/lua-api/#resp2-to-lua-type-conversion
if stored_prev_max_filled ~= false then
    prev_max_filled = stored_prev_max_filled
end

-- Return the pool size that was stored for this processor
return {new_pool_size, prev_max_filled}
