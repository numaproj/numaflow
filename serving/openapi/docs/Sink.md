# Sink

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**blackhole** | Option<[**serde_json::Value**](.md)> | Blackhole is a sink to emulate /dev/null | [optional]
**fallback** | Option<[**crate::models::AbstractSink**](AbstractSink.md)> |  | [optional]
**kafka** | Option<[**crate::models::KafkaSink**](KafkaSink.md)> |  | [optional]
**log** | Option<[**serde_json::Value**](.md)> |  | [optional]
**udsink** | Option<[**crate::models::UdSink**](UDSink.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


