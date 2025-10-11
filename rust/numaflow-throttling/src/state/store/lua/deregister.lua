--[[
Description:
  Deregisters a processor from Redis. It removes the processor's name from both the heartbeats
  and poolsize sorted sets.
  Also add the processor's name to the prev_max_filled set to track the previous max filled value in
  the token pool concerning this processor and set the TTL for the processor in the prev_max_filled set.

KEYS array:
  KEYS[1] - The base key prefix (e.g., "pipeline:vertex")

ARGV array:
  ARGV[1] - The name of the processor (e.g., "processor-alpha-1")
  ARGV[2] - The max_ever_filled value of the processor that is deregistering

Returns:
  "OK" after successfully removing the processor from both ZSETs.
]] --

-- Construct the full key names from the provided prefix
local heartbeat_key = KEYS[1] .. ":heartbeats"
local poolsize_key = KEYS[1] .. ":poolsize"
local prev_max_filled_key = KEYS[1] .. ":prev_max_filled"
local prev_max_filled_ttl_key = KEYS[1] .. ":prev_max_filled_ttl"

-- Remove the processor from both sorted sets
redis.call('ZREM', heartbeat_key, ARGV[1])
redis.call('ZREM', poolsize_key, ARGV[1])

-- Get the current timestamp for prev_max_filled ttl
local current_timestamp = redis.call('TIME')[1]

-- Add the processor to the prev_max_filled set and set the prev_max_filled value
redis.call('HSET', prev_max_filled_key, ARGV[1], ARGV[2])
-- Set the TTL for the processor in the prev_max_filled set
redis.call('ZADD', prev_max_filled_ttl_key, current_timestamp, ARGV[1])

-- Return OK to indicate successful deregistration
return "OK"