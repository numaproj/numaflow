# PbqStorage

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**empty_dir** | Option<[**models::V1PeriodEmptyDirVolumeSource**](v1.EmptyDirVolumeSource.md)> |  | [optional]
**no_store** | Option<[**serde_json::Value**](.md)> | NoStore means there will be no persistence storage and there will be data loss during pod restarts. Use this option only if you do not care about correctness (e.g., approx statistics pipeline like sampling rate, etc.). | [optional]
**persistent_volume_claim** | Option<[**models::PersistenceStrategy**](PersistenceStrategy.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


