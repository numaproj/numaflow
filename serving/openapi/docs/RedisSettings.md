# RedisSettings

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**master** | Option<**String**> | Special settings for Redis master node, will override the global settings from controller config | [optional]
**redis** | Option<**String**> | Redis settings shared by both master and slaves, will override the global settings from controller config | [optional]
**replica** | Option<**String**> | Special settings for Redis replica nodes, will override the global settings from controller config | [optional]
**sentinel** | Option<**String**> | Sentinel settings, will override the global settings from controller config | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


