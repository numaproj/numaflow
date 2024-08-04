# PipelineStatus

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**conditions** | Option<[**Vec<crate::models::V1PeriodCondition>**](v1.Condition.md)> | Conditions are the latest available observations of a resource's current state. | [optional]
**last_updated** | Option<[**crate::models::V1PeriodTime**](v1.Time.md)> |  | [optional]
**map_udf_count** | Option<**i64**> |  | [optional]
**message** | Option<**String**> |  | [optional]
**observed_generation** | Option<**i64**> | ObservedGeneration stores the generation value observed by the controller. | [optional]
**phase** | Option<**String**> |  | [optional]
**reduce_udf_count** | Option<**i64**> |  | [optional]
**sink_count** | Option<**i64**> |  | [optional]
**source_count** | Option<**i64**> |  | [optional]
**udf_count** | Option<**i64**> |  | [optional]
**vertex_count** | Option<**i64**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


