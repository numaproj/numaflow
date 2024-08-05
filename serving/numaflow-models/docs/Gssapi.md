# Gssapi

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**auth_type** | **String** | valid inputs - KRB5_USER_AUTH, KRB5_KEYTAB_AUTH  Possible enum values:  - `\"KRB5_KEYTAB_AUTH\"` represents the password method KRB5KeytabAuth = \"KRB5_KEYTAB_AUTH\" = 2  - `\"KRB5_USER_AUTH\"` represents the password method KRB5UserAuth = \"KRB5_USER_AUTH\" = 1 | 
**kerberos_config_secret** | Option<[**k8s_openapi::api::core::v1::SecretKeySelector**](k8s_openapi::api::core::v1::SecretKeySelector.md)> |  | [optional]
**keytab_secret** | Option<[**k8s_openapi::api::core::v1::SecretKeySelector**](k8s_openapi::api::core::v1::SecretKeySelector.md)> |  | [optional]
**password_secret** | Option<[**k8s_openapi::api::core::v1::SecretKeySelector**](k8s_openapi::api::core::v1::SecretKeySelector.md)> |  | [optional]
**realm** | **String** |  | 
**service_name** | **String** |  | 
**username_secret** | [**k8s_openapi::api::core::v1::SecretKeySelector**](k8s_openapi::api::core::v1::SecretKeySelector.md) |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


