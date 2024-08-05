# VertexLimits

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**buffer_max_length** | Option<**i64**> | BufferMaxLength is used to define the max length of a buffer. It overrides the settings from pipeline limits. | [optional]
**buffer_usage_limit** | Option<**i64**> | BufferUsageLimit is used to define the percentage of the buffer usage limit, a valid value should be less than 100, for example, 85. It overrides the settings from pipeline limits. | [optional]
**read_batch_size** | Option<**i64**> | Read batch size from the source or buffer. It overrides the settings from pipeline limits. | [optional]
**read_timeout** | Option<[**kube::core::Duration**](kube::core::Duration.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


