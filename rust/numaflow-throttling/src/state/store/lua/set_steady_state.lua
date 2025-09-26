--[[
Description:
  Simply sets the steady state status of the processor in redis.

KEYS array:
  KEYS[1] - The base key prefix (e.g., "pipeline:vertex")

ARGV array:
  ARGV[1] - The name of the processor (e.g., "processor-alpha-1")
  ARGV[2] - Whether the processor is in steady state (true/false)

Returns:
  "OK" after successfully updating the steady state of the processor in Hashmap.
]]--

-- Construct the full key names from the provided prefix
local steady_state_key = KEYS[1] .. ":steady_states"


-- Remove the processor from both sorted sets
redis.call('HSET', steady_state_key, ARGV[1], ARGV[2])

-- Return OK to indicate successful deregistration
return "OK"