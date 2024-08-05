# GetRedisStatefulSetSpecReq

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**conf_config_map_name** | **String** |  | 
**credential_secret_name** | **String** |  | 
**default_resources** | [**k8s_openapi::api::core::v1::ResourceRequirements**](k8s_openapi::api::core::v1::ResourceRequirements.md) |  | 
**health_config_map_name** | **String** |  | 
**init_container_image** | **String** |  | 
**labels** | **::std::collections::HashMap<String, String>** |  | 
**metrics_exporter_image** | **String** |  | 
**pvc_name_if_needed** | **String** |  | 
**redis_container_port** | **i32** |  | 
**redis_image** | **String** |  | 
**redis_metrics_container_port** | **i32** |  | 
**scripts_config_map_name** | **String** |  | 
**sentinel_container_port** | **i32** |  | 
**sentinel_image** | **String** |  | 
**service_name** | **String** |  | 
**tls_enabled** | **bool** |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


