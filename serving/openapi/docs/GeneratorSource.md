# GeneratorSource

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**duration** | Option<[**kube::core::Duration**](kube::core::Duration.md)> |  | [optional]
**jitter** | Option<[**kube::core::Duration**](kube::core::Duration.md)> |  | [optional]
**key_count** | Option<**i32**> | KeyCount is the number of unique keys in the payload | [optional]
**msg_size** | Option<**i32**> | Size of each generated message | [optional]
**rpu** | Option<**i64**> |  | [optional]
**value** | Option<**i64**> | Value is an optional uint64 value to be written in to the payload | [optional]
**value_blob** | Option<**String**> | ValueBlob is an optional string which is the base64 encoding of direct payload to send. This is useful for attaching a GeneratorSource to a true pipeline to test load behavior with true messages without requiring additional work to generate messages through the external source if present, the Value and MsgSize fields will be ignored. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


