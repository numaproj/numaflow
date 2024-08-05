# SideInputsManagerTemplate

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**affinity** | Option<[**k8s_openapi::api::core::v1::Affinity**](k8s_openapi::api::core::v1::Affinity.md)> |  | [optional]
**automount_service_account_token** | Option<**bool**> | AutomountServiceAccountToken indicates whether a service account token should be automatically mounted. | [optional]
**container_template** | Option<[**crate::models::ContainerTemplate**](ContainerTemplate.md)> |  | [optional]
**dns_config** | Option<[**k8s_openapi::api::core::v1::PodDNSConfig**](k8s_openapi::api::core::v1::PodDNSConfig.md)> |  | [optional]
**dns_policy** | Option<**String**> | Set DNS policy for the pod. Defaults to \"ClusterFirst\". Valid values are 'ClusterFirstWithHostNet', 'ClusterFirst', 'Default' or 'None'. DNS parameters given in DNSConfig will be merged with the policy selected with DNSPolicy. To have DNS options set along with hostNetwork, you have to specify DNS policy explicitly to 'ClusterFirstWithHostNet'. | [optional]
**image_pull_secrets** | Option<[**Vec<k8s_openapi::api::core::v1::LocalObjectReference>**](k8s_openapi::api::core::v1::LocalObjectReference.md)> | ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec. If specified, these secrets will be passed to individual puller implementations for them to use. For example, in the case of docker, only DockerConfig type secrets are honored. More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod | [optional]
**init_container_template** | Option<[**crate::models::ContainerTemplate**](ContainerTemplate.md)> |  | [optional]
**metadata** | Option<[**crate::models::Metadata**](Metadata.md)> |  | [optional]
**node_selector** | Option<**::std::collections::HashMap<String, String>**> | NodeSelector is a selector which must be true for the pod to fit on a node. Selector which must match a node's labels for the pod to be scheduled on that node. More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/ | [optional]
**priority** | Option<**i32**> | The priority value. Various system components use this field to find the priority of the Redis pod. When Priority Admission Controller is enabled, it prevents users from setting this field. The admission controller populates this field from PriorityClassName. The higher the value, the higher the priority. More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/ | [optional]
**priority_class_name** | Option<**String**> | If specified, indicates the Redis pod's priority. \"system-node-critical\" and \"system-cluster-critical\" are two special keywords which indicate the highest priorities with the former being the highest priority. Any other name must be defined by creating a PriorityClass object with that name. If not specified, the pod priority will be default or zero if there is no default. More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/ | [optional]
**runtime_class_name** | Option<**String**> | RuntimeClassName refers to a RuntimeClass object in the node.k8s.io group, which should be used to run this pod.  If no RuntimeClass resource matches the named class, the pod will not be run. If unset or empty, the \"legacy\" RuntimeClass will be used, which is an implicit class with an empty definition that uses the default runtime handler. More info: https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class | [optional]
**security_context** | Option<[**k8s_openapi::api::core::v1::PodSecurityContext**](k8s_openapi::api::core::v1::PodSecurityContext.md)> |  | [optional]
**service_account_name** | Option<**String**> | ServiceAccountName applied to the pod | [optional]
**tolerations** | Option<[**Vec<k8s_openapi::api::core::v1::Toleration>**](k8s_openapi::api::core::v1::Toleration.md)> | If specified, the pod's tolerations. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


