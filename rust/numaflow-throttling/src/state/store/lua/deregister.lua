--[[
Description:
  Deregisters a processor from Redis. It removes the processor's name from both the heartbeats
  and poolsize sorted sets.

KEYS array:
  KEYS[1] - The base key prefix (e.g., "pipeline:vertex")

ARGV array:
  ARGV[1] - The name of the processor (e.g., "processor-alpha-1")

Returns:
  "OK" after successfully removing the processor from both ZSETs.
]]--

-- Construct the full key names from the provided prefix
local heartbeat_key = KEYS[1] .. ":heartbeats"
local poolsize_key = KEYS[1] .. ":poolsize"

-- Remove the processor from both sorted sets
redis.call('ZREM', heartbeat_key, ARGV[1])
redis.call('ZREM', poolsize_key, ARGV[1])

-- Return OK to indicate successful deregistration
return "OK"