# CombinedEdge

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**conditions** | Option<[**crate::models::ForwardConditions**](ForwardConditions.md)> |  | [optional]
**from** | **String** |  | 
**from_vertex_limits** | Option<[**crate::models::VertexLimits**](VertexLimits.md)> |  | [optional]
**from_vertex_partition_count** | Option<**i32**> | The number of partitions of the from vertex, if not provided, the default value is set to \"1\". | [optional]
**from_vertex_type** | **String** | From vertex type. | 
**on_full** | Option<**String**> | OnFull specifies the behaviour for the write actions when the inter step buffer is full. There are currently two options, retryUntilSuccess and discardLatest. if not provided, the default value is set to \"retryUntilSuccess\" | [optional]
**to** | **String** |  | 
**to_vertex_limits** | Option<[**crate::models::VertexLimits**](VertexLimits.md)> |  | [optional]
**to_vertex_partition_count** | Option<**i32**> | The number of partitions of the to vertex, if not provided, the default value is set to \"1\". | [optional]
**to_vertex_type** | **String** | To vertex type. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


