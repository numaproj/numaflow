# RedisConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**master_name** | Option<**String**> | Only required when Sentinel is used | [optional]
**password** | Option<[**k8s_openapi::api::core::v1::SecretKeySelector**](k8s_openapi::api::core::v1::SecretKeySelector.md)> |  | [optional]
**sentinel_password** | Option<[**k8s_openapi::api::core::v1::SecretKeySelector**](k8s_openapi::api::core::v1::SecretKeySelector.md)> |  | [optional]
**sentinel_url** | Option<**String**> | Sentinel URL, will be ignored if Redis URL is provided | [optional]
**url** | Option<**String**> | Redis URL | [optional]
**user** | Option<**String**> | Redis user | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


