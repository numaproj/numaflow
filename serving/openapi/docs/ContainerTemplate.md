# ContainerTemplate

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**env** | Option<[**Vec<k8s_openapi::api::core::v1::EnvVar>**](k8s_openapi::api::core::v1::EnvVar.md)> |  | [optional]
**env_from** | Option<[**Vec<k8s_openapi::api::core::v1::EnvFromSource>**](k8s_openapi::api::core::v1::EnvFromSource.md)> |  | [optional]
**image_pull_policy** | Option<**String**> |  | [optional]
**resources** | Option<[**k8s_openapi::api::core::v1::ResourceRequirements**](k8s_openapi::api::core::v1::ResourceRequirements.md)> |  | [optional]
**security_context** | Option<[**k8s_openapi::api::core::v1::SecurityContext**](k8s_openapi::api::core::v1::SecurityContext.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


