# Container

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**args** | Option<**Vec<String>**> |  | [optional]
**command** | Option<**Vec<String>**> |  | [optional]
**env** | Option<[**Vec<models::V1PeriodEnvVar>**](v1.EnvVar.md)> |  | [optional]
**env_from** | Option<[**Vec<models::V1PeriodEnvFromSource>**](v1.EnvFromSource.md)> |  | [optional]
**image** | Option<**String**> |  | [optional]
**image_pull_policy** | Option<**String**> |  | [optional]
**resources** | Option<[**models::V1PeriodResourceRequirements**](v1.ResourceRequirements.md)> |  | [optional]
**security_context** | Option<[**models::V1PeriodSecurityContext**](v1.SecurityContext.md)> |  | [optional]
**volume_mounts** | Option<[**Vec<models::V1PeriodVolumeMount>**](v1.VolumeMount.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


