# InterStepBufferServiceStatus

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**conditions** | Option<[**Vec<models::V1PeriodCondition>**](v1.Condition.md)> | Conditions are the latest available observations of a resource's current state. | [optional]
**config** | Option<[**models::BufferServiceConfig**](BufferServiceConfig.md)> |  | [optional]
**message** | Option<**String**> |  | [optional]
**observed_generation** | Option<**i64**> | ObservedGeneration stores the generation value observed by the controller. | [optional]
**phase** | Option<**String**> |  | [optional]
**r#type** | Option<**String**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


