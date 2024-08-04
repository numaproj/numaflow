# VertexStatus

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**conditions** | Option<[**Vec<models::V1PeriodCondition>**](v1.Condition.md)> | Conditions are the latest available observations of a resource's current state. | [optional]
**last_scaled_at** | Option<[**models::V1PeriodTime**](v1.Time.md)> |  | [optional]
**message** | Option<**String**> |  | [optional]
**observed_generation** | Option<**i64**> | ObservedGeneration stores the generation value observed by the controller. | [optional]
**phase** | **String** |  | 
**reason** | Option<**String**> |  | [optional]
**replicas** | **i64** |  | 
**selector** | Option<**String**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


