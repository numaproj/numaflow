

# RedisSettings


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**master** | **String** | Special settings for Redis master node, will override the global settings from controller config |  [optional]
**redis** | **String** | Redis settings shared by both master and slaves, will override the global settings from controller config |  [optional]
**replica** | **String** | Special settings for Redis replica nodes, will override the global settings from controller config |  [optional]
**sentinel** | **String** | Sentinel settings, will override the global settings from controller config |  [optional]



