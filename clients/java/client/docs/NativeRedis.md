

# NativeRedis


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**affinity** | **V1Affinity** |  |  [optional]
**imagePullSecrets** | **List&lt;V1LocalObjectReference&gt;** | ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec. If specified, these secrets will be passed to individual puller implementations for them to use. For example, in the case of docker, only DockerConfig type secrets are honored. More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod |  [optional]
**metadata** | [**Metadata**](Metadata.md) |  |  [optional]
**metricsContainerTemplate** | [**ContainerTemplate**](ContainerTemplate.md) |  |  [optional]
**nodeSelector** | **Map&lt;String, String&gt;** | NodeSelector is a selector which must be true for the pod to fit on a node. Selector which must match a node&#39;s labels for the pod to be scheduled on that node. More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/ |  [optional]
**persistence** | [**PersistenceStrategy**](PersistenceStrategy.md) |  |  [optional]
**priority** | **Integer** | The priority value. Various system components use this field to find the priority of the Redis pod. When Priority Admission Controller is enabled, it prevents users from setting this field. The admission controller populates this field from PriorityClassName. The higher the value, the higher the priority. More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/ |  [optional]
**priorityClassName** | **String** | If specified, indicates the Redis pod&#39;s priority. \&quot;system-node-critical\&quot; and \&quot;system-cluster-critical\&quot; are two special keywords which indicate the highest priorities with the former being the highest priority. Any other name must be defined by creating a PriorityClass object with that name. If not specified, the pod priority will be default or zero if there is no default. More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/ |  [optional]
**redisContainerTemplate** | [**ContainerTemplate**](ContainerTemplate.md) |  |  [optional]
**replicas** | **Integer** | Redis StatefulSet size |  [optional]
**securityContext** | **V1PodSecurityContext** |  |  [optional]
**sentinelContainerTemplate** | [**ContainerTemplate**](ContainerTemplate.md) |  |  [optional]
**serviceAccountName** | **String** | ServiceAccountName to apply to the StatefulSet |  [optional]
**settings** | [**RedisSettings**](RedisSettings.md) |  |  [optional]
**tolerations** | **List&lt;V1Toleration&gt;** | If specified, the pod&#39;s tolerations. |  [optional]
**version** | **String** | Redis version, such as \&quot;6.0.16\&quot; |  [optional]



