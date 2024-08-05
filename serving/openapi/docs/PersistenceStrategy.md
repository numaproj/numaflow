# PersistenceStrategy

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**access_mode** | Option<**String**> | Available access modes such as ReadWriteOnce, ReadWriteMany https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes | [optional]
**storage_class_name** | Option<**String**> | Name of the StorageClass required by the claim. More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#class-1 | [optional]
**volume_size** | Option<[**k8s_openapi::apimachinery::pkg::api::resource::Quantity**](k8s_openapi::apimachinery::pkg::api::resource::Quantity.md)> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


