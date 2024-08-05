# GetJetStreamStatefulSetSpecReq

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**client_port** | **i32** |  | 
**cluster_port** | **i32** |  | 
**config_map_name** | **String** |  | 
**config_reloader_image** | **String** |  | 
**default_resources** | [**k8s_openapi::api::core::v1::ResourceRequirements**](k8s_openapi::api::core::v1::ResourceRequirements.md) |  | 
**labels** | **::std::collections::HashMap<String, String>** |  | 
**metrics_exporter_image** | **String** |  | 
**metrics_port** | **i32** |  | 
**monitor_port** | **i32** |  | 
**nats_image** | **String** |  | 
**pvc_name_if_needed** | **String** |  | 
**server_auth_secret_name** | **String** |  | 
**server_encryption_secret_name** | **String** |  | 
**service_name** | **String** |  | 
**start_command** | **String** |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


