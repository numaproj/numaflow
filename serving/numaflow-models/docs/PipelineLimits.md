# PipelineLimits

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**buffer_max_length** | Option<**i64**> | BufferMaxLength is used to define the max length of a buffer. Only applies to UDF and Source vertices as only they do buffer write. It can be overridden by the settings in vertex limits. | [optional]
**buffer_usage_limit** | Option<**i64**> | BufferUsageLimit is used to define the percentage of the buffer usage limit, a valid value should be less than 100, for example, 85. Only applies to UDF and Source vertices as only they do buffer write. It will be overridden by the settings in vertex limits. | [optional]
**read_batch_size** | Option<**i64**> | Read batch size for all the vertices in the pipeline, can be overridden by the vertex's limit settings. | [optional]
**read_timeout** | Option<[**kube::core::Duration**](kube::core::Duration.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


