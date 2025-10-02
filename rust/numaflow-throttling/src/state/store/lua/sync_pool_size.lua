--[[
Description:
  Updates processor heartbeats and pool sizes, cleans up stale entries,
  and checks if all active processors agree on the pool size.

KEYS array:
  KEYS[1] - The base key prefix (e.g., "pipeline:vertex")

ARGV array:
  ARGV[1] - The name of the processor (e.g., "processor-alpha-1")
  ARGV[2] - The current Unix timestamp (score for the heartbeat)
  ARGV[3] - The total known pool size (score for the pool size)
  ARGV[4] - The stale age in seconds

Returns:
  A table with three elements:
  - {"AGREE", "pool_size", "active_processor_count"} if all active processors report the same pool size.
  - {"DISAGREE", "largest_pool_size", "active_processor_count"} if there is a conflict or no active processors.
--]]

-- Define the stale age in seconds
local STALE_AGE = tonumber(ARGV[4])

-- Construct the full key names from the provided prefix
local heartbeat_key = KEYS[1] .. ":heartbeats"
local poolsize_key = KEYS[1] .. ":poolsize"

-- Step 1: Insert or update the data for the current processor
redis.call('ZADD', heartbeat_key, ARGV[2], ARGV[1])
redis.call('ZADD', poolsize_key, ARGV[3], ARGV[1])

-- Step 2: Clean up stale processors
-- A processor is stale if its last heartbeat is older than the threshold
local stale_threshold = tonumber(ARGV[2]) - STALE_AGE

-- Find all members with a score (timestamp) older than the threshold
local stale_members = redis.call('ZRANGEBYSCORE', heartbeat_key, '-inf', '(' .. stale_threshold)

-- If any stale members were found, remove them from both sorted sets
if #stale_members > 0 then
    -- The unpack function passes the table elements as individual arguments to ZREM
    redis.call('ZREM', heartbeat_key, unpack(stale_members))
    redis.call('ZREM', poolsize_key, unpack(stale_members))
end

-- Step 3: Check for agreement among the remaining (active) processors
local active_processor_count = redis.call('ZCARD', heartbeat_key)
local poolsize_count = redis.call('ZCARD', poolsize_key)

-- If no processors are left, or if the counts don't match, we cannot agree
if active_processor_count == 0 or active_processor_count ~= poolsize_count then
    local max_pool_size = "0"
    -- Try to get the largest pool size if the poolsize set isn't empty
    if poolsize_count > 0 then
        local max_pool_result = redis.call('ZRANGE', poolsize_key, -1, -1, 'WITHSCORES')
        if max_pool_result and max_pool_result[2] then
            max_pool_size = max_pool_result[2]
        end
    end
    return { "DISAGREE", max_pool_size, poolsize_count }
end

-- Retrieve the smallest and largest pool sizes from the active pool
local min_pool_result = redis.call('ZRANGE', poolsize_key, 0, 0, 'WITHSCORES')
local max_pool_result = redis.call('ZRANGE', poolsize_key, -1, -1, 'WITHSCORES')

-- The scores are the second element in the results table
local min_pool_size = min_pool_result[2]
local max_pool_size = max_pool_result[2]

-- Compare scores to check for agreement
if min_pool_size == max_pool_size then
    -- Success: All active processors report the same pool size
    return { "AGREE", min_pool_size, poolsize_count }
else
    -- Failure: Active processors report different pool sizes
    return { "DISAGREE", max_pool_size, poolsize_count }
end