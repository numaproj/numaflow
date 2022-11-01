<p>
Packages:
</p>
<ul>
<li>
<a href="#numaflow.numaproj.io%2fv1alpha1">numaflow.numaproj.io/v1alpha1</a>
</li>
</ul>
<h2 id="numaflow.numaproj.io/v1alpha1">
numaflow.numaproj.io/v1alpha1
</h2>
<p>
</p>
Resource Types:
<ul>
</ul>
<h3 id="numaflow.numaproj.io/v1alpha1.AbstractVertex">
AbstractVertex
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec">PipelineSpec</a>,
<a href="#numaflow.numaproj.io/v1alpha1.VertexSpec">VertexSpec</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>name</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>source</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Source"> Source </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>sink</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Sink"> Sink </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>containerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>udf</code></br> <em> <a href="#numaflow.numaproj.io/v1alpha1.UDF">
UDF </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>metadata</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Metadata"> Metadata </a> </em>
</td>
<td>
<p>
Metadata sets the pods’s metadata, i.e. annotations and labels
</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code></br> <em> map\[string\]string </em>
</td>
<td>
<em>(Optional)</em>
<p>
NodeSelector is a selector which must be true for the pod to fit on a
node. Selector which must match a node’s labels for the pod to be
scheduled on that node. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/assign-pod-node/">https://kubernetes.io/docs/concepts/configuration/assign-pod-node/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>tolerations</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#toleration-v1-core">
\[\]Kubernetes core/v1.Toleration </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
If specified, the pod’s tolerations.
</p>
</td>
</tr>
<tr>
<td>
<code>securityContext</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#podsecuritycontext-v1-core">
Kubernetes core/v1.PodSecurityContext </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
SecurityContext holds pod-level security attributes and common container
settings. Optional: Defaults to empty. See type description for default
values of each field.
</p>
</td>
</tr>
<tr>
<td>
<code>imagePullSecrets</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#localobjectreference-v1-core">
\[\]Kubernetes core/v1.LocalObjectReference </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
ImagePullSecrets is an optional list of references to secrets in the
same namespace to use for pulling any of the images used by this
PodSpec. If specified, these secrets will be passed to individual puller
implementations for them to use. For example, in the case of docker,
only DockerConfig type secrets are honored. More info:
<a href="https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod">https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod</a>
</p>
</td>
</tr>
<tr>
<td>
<code>priorityClassName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
If specified, indicates the Redis pod’s priority. “system-node-critical”
and “system-cluster-critical” are two special keywords which indicate
the highest priorities with the former being the highest priority. Any
other name must be defined by creating a PriorityClass object with that
name. If not specified, the pod priority will be default or zero if
there is no default. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/">https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>priority</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
The priority value. Various system components use this field to find the
priority of the Redis pod. When Priority Admission Controller is
enabled, it prevents users from setting this field. The admission
controller populates this field from PriorityClassName. The higher the
value, the higher the priority. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/">https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>affinity</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#affinity-v1-core">
Kubernetes core/v1.Affinity </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
The pod’s scheduling constraints More info:
<a href="https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/">https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccountName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
ServiceAccountName to apply to the StatefulSet
</p>
</td>
</tr>
<tr>
<td>
<code>volumes</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volume-v1-core">
\[\]Kubernetes core/v1.Volume </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>limits</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexLimits"> VertexLimits </a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>
Limits define the limitations such as buffer read batch size for all the
vertices of a pipeline, will override pipeline level settings
</p>
</td>
</tr>
<tr>
<td>
<code>scale</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Scale"> Scale </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Settings for autoscaling
</p>
</td>
</tr>
<tr>
<td>
<code>initContainers</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#container-v1-core">
\[\]Kubernetes core/v1.Container </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
List of init containers belonging to the pod. More info:
<a href="https://kubernetes.io/docs/concepts/workloads/pods/init-containers/">https://kubernetes.io/docs/concepts/workloads/pods/init-containers/</a>
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Authorization">
Authorization
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.HTTPSource">HTTPSource</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>token</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
A secret selector which contains bearer token To use this, the client
needs to add “Authorization: Bearer <token>” in the header
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Buffer">
Buffer
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>Name</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>Type</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.BufferType"> BufferType </a>
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.BufferServiceConfig">
BufferServiceConfig
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferServiceStatus">InterStepBufferServiceStatus</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>redis</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisConfig"> RedisConfig </a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>jetstream</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamConfig">
JetStreamConfig </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.BufferType">
BufferType (<code>string</code> alias)
</p>
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Buffer">Buffer</a>)
</p>
<p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.ConditionType">
ConditionType (<code>string</code> alias)
</p>
</h3>
<p>
<p>
ConditionType is a valid value of Condition.Type
</p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.Container">
Container
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.UDF">UDF</a>,
<a href="#numaflow.numaproj.io/v1alpha1.UDSink">UDSink</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>image</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>command</code></br> <em> \[\]string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>args</code></br> <em> \[\]string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>env</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#envvar-v1-core">
\[\]Kubernetes core/v1.EnvVar </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>volumeMounts</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#volumemount-v1-core">
\[\]Kubernetes core/v1.VolumeMount </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>resources</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.DaemonTemplate">DaemonTemplate</a>,
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamBufferService">JetStreamBufferService</a>,
<a href="#numaflow.numaproj.io/v1alpha1.NativeRedis">NativeRedis</a>)
</p>
<p>
<p>
ContainerTemplate defines customized spec for a container
</p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>resources</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>imagePullPolicy</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#pullpolicy-v1-core">
Kubernetes core/v1.PullPolicy </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>securityContext</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#securitycontext-v1-core">
Kubernetes core/v1.SecurityContext </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>env</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#envvar-v1-core">
\[\]Kubernetes core/v1.EnvVar </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.DaemonTemplate">
DaemonTemplate
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Templates">Templates</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>containerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>metadata</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Metadata"> Metadata </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Metadata sets the pods’s metadata, i.e. annotations and labels
</p>
</td>
</tr>
<tr>
<td>
<code>replicas</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Replicas is the number of desired replicas of the Deployment. This is a
pointer to distinguish between explicit zero and unspecified. Defaults
to 1. More info:
<a href="https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#what-is-a-replicationcontroller">https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#what-is-a-replicationcontroller</a>
</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code></br> <em> map\[string\]string </em>
</td>
<td>
<em>(Optional)</em>
<p>
NodeSelector is a selector which must be true for the pod to fit on a
node. Selector which must match a node’s labels for the pod to be
scheduled on that node. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/assign-pod-node/">https://kubernetes.io/docs/concepts/configuration/assign-pod-node/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>tolerations</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#toleration-v1-core">
\[\]Kubernetes core/v1.Toleration </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
If specified, the pod’s tolerations.
</p>
</td>
</tr>
<tr>
<td>
<code>securityContext</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#podsecuritycontext-v1-core">
Kubernetes core/v1.PodSecurityContext </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
SecurityContext holds pod-level security attributes and common container
settings. Optional: Defaults to empty. See type description for default
values of each field.
</p>
</td>
</tr>
<tr>
<td>
<code>imagePullSecrets</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#localobjectreference-v1-core">
\[\]Kubernetes core/v1.LocalObjectReference </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
ImagePullSecrets is an optional list of references to secrets in the
same namespace to use for pulling any of the images used by this
PodSpec. If specified, these secrets will be passed to individual puller
implementations for them to use. For example, in the case of docker,
only DockerConfig type secrets are honored. More info:
<a href="https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod">https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod</a>
</p>
</td>
</tr>
<tr>
<td>
<code>priorityClassName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
If specified, indicates the Redis pod’s priority. “system-node-critical”
and “system-cluster-critical” are two special keywords which indicate
the highest priorities with the former being the highest priority. Any
other name must be defined by creating a PriorityClass object with that
name. If not specified, the pod priority will be default or zero if
there is no default. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/">https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>priority</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
The priority value. Various system components use this field to find the
priority of the Redis pod. When Priority Admission Controller is
enabled, it prevents users from setting this field. The admission
controller populates this field from PriorityClassName. The higher the
value, the higher the priority. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/">https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>affinity</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#affinity-v1-core">
Kubernetes core/v1.Affinity </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
The pod’s scheduling constraints More info:
<a href="https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/">https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccountName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
ServiceAccountName to apply to the Deployment
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Edge">
Edge
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec">PipelineSpec</a>,
<a href="#numaflow.numaproj.io/v1alpha1.VertexSpec">VertexSpec</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>from</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>to</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>conditions</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ForwardConditions">
ForwardConditions </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Conditional forwarding, only allowed when “From” is a Sink or UDF.
</p>
</td>
</tr>
<tr>
<td>
<code>limits</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.EdgeLimits"> EdgeLimits </a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>
Limits define the limitations such as buffer read batch size for the
edge, will override pipeline level settings.
</p>
</td>
</tr>
<tr>
<td>
<code>parallelism</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Parallelism is only effective when the “to” vertex is a reduce vertex,
if it’s provided, the default value is set to “1”. Parallelism is
ignored when the “to” vertex is not a reduce vertex.
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.EdgeLimits">
EdgeLimits
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge">Edge</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>bufferMaxLength</code></br> <em> uint64 </em>
</td>
<td>
<em>(Optional)</em>
<p>
BufferMaxLength is used to define the max length of a buffer. It
overrides the settings from pipeline limits.
</p>
</td>
</tr>
<tr>
<td>
<code>bufferUsageLimit</code></br> <em> uint32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
BufferUsageLimit is used to define the percentage of the buffer usage
limit, a valid value should be less than 100, for example, 85. It
overrides the settings from pipeline limits.
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.FixedWindow">
FixedWindow
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Window">Window</a>)
</p>
<p>
<p>
FixedWindow describes a fixed window
</p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>length</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.ForwardConditions">
ForwardConditions
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge">Edge</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>keyIn</code></br> <em> \[\]string </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Function">
Function
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.UDF">UDF</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>name</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>args</code></br> <em> \[\]string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>kwargs</code></br> <em> map\[string\]string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.GeneratorSource">
GeneratorSource
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Source">Source</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>rpu</code></br> <em> int64 </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>duration</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>msgSize</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Size of each generated message
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.GetDaemonDeploymentReq">
GetDaemonDeploymentReq
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>ISBSvcType</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ISBSvcType"> ISBSvcType </a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>Image</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>PullPolicy</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#pullpolicy-v1-core">
Kubernetes core/v1.PullPolicy </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>Env</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#envvar-v1-core">
\[\]Kubernetes core/v1.EnvVar </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.GetJetStreamServiceSpecReq">
GetJetStreamServiceSpecReq
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>Labels</code></br> <em> map\[string\]string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ClusterPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ClientPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>MonitorPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>MetricsPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.GetJetStreamStatefulSetSpecReq">
GetJetStreamStatefulSetSpecReq
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>ServiceName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>Labels</code></br> <em> map\[string\]string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>NatsImage</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>MetricsExporterImage</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ConfigReloaderImage</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ClusterPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ClientPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>MonitorPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>MetricsPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ServerAuthSecretName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ServerEncryptionSecretName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ConfigMapName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>PvcNameIfNeeded</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>StartCommand</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.GetRedisServiceSpecReq">
GetRedisServiceSpecReq
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>Labels</code></br> <em> map\[string\]string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>RedisContainerPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>SentinelContainerPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.GetRedisStatefulSetSpecReq">
GetRedisStatefulSetSpecReq
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>ServiceName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>Labels</code></br> <em> map\[string\]string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>RedisImage</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>SentinelImage</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>MetricsExporterImage</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>InitContainerImage</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>RedisContainerPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>SentinelContainerPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>RedisMetricsContainerPort</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>CredentialSecretName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>TLSEnabled</code></br> <em> bool </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>PvcNameIfNeeded</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ConfConfigMapName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>ScriptsConfigMapName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>HealthConfigMapName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.GetVertexPodSpecReq">
GetVertexPodSpecReq
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>ISBSvcType</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ISBSvcType"> ISBSvcType </a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>Image</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>PullPolicy</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#pullpolicy-v1-core">
Kubernetes core/v1.PullPolicy </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>Env</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#envvar-v1-core">
\[\]Kubernetes core/v1.EnvVar </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.GroupBy">
GroupBy
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.UDF">UDF</a>)
</p>
<p>
<p>
GroupBy indicates it is a reducer UDF
</p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>window</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Window"> Window </a> </em>
</td>
<td>
<p>
Window describes the windowing strategy.
</p>
</td>
</tr>
<tr>
<td>
<code>keyed</code></br> <em> bool </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>storage</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PBQStorage"> PBQStorage </a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>
Storage is used to define the PBQ storage for a reduce vertex.
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.HTTPSource">
HTTPSource
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Source">Source</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>auth</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Authorization"> Authorization
</a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>service</code></br> <em> bool </em>
</td>
<td>
<em>(Optional)</em>
<p>
Whether to create a ClusterIP Service
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.ISBSvcPhase">
ISBSvcPhase (<code>string</code> alias)
</p>
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferServiceStatus">InterStepBufferServiceStatus</a>)
</p>
<p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.ISBSvcType">
ISBSvcType (<code>string</code> alias)
</p>
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.GetDaemonDeploymentReq">GetDaemonDeploymentReq</a>,
<a href="#numaflow.numaproj.io/v1alpha1.GetVertexPodSpecReq">GetVertexPodSpecReq</a>,
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferServiceStatus">InterStepBufferServiceStatus</a>)
</p>
<p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.InterStepBufferService">
InterStepBufferService
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>metadata</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#objectmeta-v1-meta">
Kubernetes meta/v1.ObjectMeta </a> </em>
</td>
<td>
Refer to the Kubernetes API documentation for the fields of the
<code>metadata</code> field.
</td>
</tr>
<tr>
<td>
<code>spec</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferServiceSpec">
InterStepBufferServiceSpec </a> </em>
</td>
<td>
<br/> <br/>
<table>
<tr>
<td>
<code>redis</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisBufferService">
RedisBufferService </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>jetstream</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamBufferService">
JetStreamBufferService </a> </em>
</td>
<td>
</td>
</tr>
</table>
</td>
</tr>
<tr>
<td>
<code>status</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferServiceStatus">
InterStepBufferServiceStatus </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.InterStepBufferServiceSpec">
InterStepBufferServiceSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferService">InterStepBufferService</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>redis</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisBufferService">
RedisBufferService </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>jetstream</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamBufferService">
JetStreamBufferService </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.InterStepBufferServiceStatus">
InterStepBufferServiceStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferService">InterStepBufferService</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>Status</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Status"> Status </a> </em>
</td>
<td>
<p>
(Members of <code>Status</code> are embedded into this type.)
</p>
</td>
</tr>
<tr>
<td>
<code>phase</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ISBSvcPhase"> ISBSvcPhase </a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>message</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>config</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.BufferServiceConfig">
BufferServiceConfig </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>type</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ISBSvcType"> ISBSvcType </a>
</em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.JetStreamBufferService">
JetStreamBufferService
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferServiceSpec">InterStepBufferServiceSpec</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>version</code></br> <em> string </em>
</td>
<td>
<p>
JetStream version, such as “2.7.1”
</p>
</td>
</tr>
<tr>
<td>
<code>replicas</code></br> <em> int32 </em>
</td>
<td>
<p>
Redis StatefulSet size
</p>
</td>
</tr>
<tr>
<td>
<code>containerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
ContainerTemplate contains customized spec for NATS container
</p>
</td>
</tr>
<tr>
<td>
<code>reloaderContainerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
ReloaderContainerTemplate contains customized spec for config reloader
container
</p>
</td>
</tr>
<tr>
<td>
<code>metricsContainerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
MetricsContainerTemplate contains customized spec for metrics container
</p>
</td>
</tr>
<tr>
<td>
<code>persistence</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PersistenceStrategy">
PersistenceStrategy </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>metadata</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Metadata"> Metadata </a> </em>
</td>
<td>
<p>
Metadata sets the pods’s metadata, i.e. annotations and labels
</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code></br> <em> map\[string\]string </em>
</td>
<td>
<em>(Optional)</em>
<p>
NodeSelector is a selector which must be true for the pod to fit on a
node. Selector which must match a node’s labels for the pod to be
scheduled on that node. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/assign-pod-node/">https://kubernetes.io/docs/concepts/configuration/assign-pod-node/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>tolerations</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#toleration-v1-core">
\[\]Kubernetes core/v1.Toleration </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
If specified, the pod’s tolerations.
</p>
</td>
</tr>
<tr>
<td>
<code>securityContext</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#podsecuritycontext-v1-core">
Kubernetes core/v1.PodSecurityContext </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
SecurityContext holds pod-level security attributes and common container
settings. Optional: Defaults to empty. See type description for default
values of each field.
</p>
</td>
</tr>
<tr>
<td>
<code>imagePullSecrets</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#localobjectreference-v1-core">
\[\]Kubernetes core/v1.LocalObjectReference </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
ImagePullSecrets is an optional list of references to secrets in the
same namespace to use for pulling any of the images used by this
PodSpec. If specified, these secrets will be passed to individual puller
implementations for them to use. For example, in the case of docker,
only DockerConfig type secrets are honored. More info:
<a href="https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod">https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod</a>
</p>
</td>
</tr>
<tr>
<td>
<code>priorityClassName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
If specified, indicates the Redis pod’s priority. “system-node-critical”
and “system-cluster-critical” are two special keywords which indicate
the highest priorities with the former being the highest priority. Any
other name must be defined by creating a PriorityClass object with that
name. If not specified, the pod priority will be default or zero if
there is no default. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/">https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>priority</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
The priority value. Various system components use this field to find the
priority of the Redis pod. When Priority Admission Controller is
enabled, it prevents users from setting this field. The admission
controller populates this field from PriorityClassName. The higher the
value, the higher the priority. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/">https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>affinity</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#affinity-v1-core">
Kubernetes core/v1.Affinity </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
The pod’s scheduling constraints More info:
<a href="https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/">https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccountName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
ServiceAccountName to apply to the StatefulSet
</p>
</td>
</tr>
<tr>
<td>
<code>settings</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
JetStream configuration, if not specified, global settings in
numaflow-controller-config will be used. See
<a href="https://docs.nats.io/running-a-nats-service/configuration#jetstream">https://docs.nats.io/running-a-nats-service/configuration#jetstream</a>.
Only configure “max_memory_store” or “max_file_store”, do not set
“store_dir” as it has been hardcoded.
</p>
</td>
</tr>
<tr>
<td>
<code>startArgs</code></br> <em> \[\]string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Optional arguments to start nats-server. For example, “-D” to enable
debugging output, “-DV” to enable debugging and tracing. Check
<a href="https://docs.nats.io/">https://docs.nats.io/</a> for all the
available arguments.
</p>
</td>
</tr>
<tr>
<td>
<code>bufferConfig</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Optional configuration for the streams, consumers and buckets to be
created in this JetStream service, if specified, it will be merged with
the default configuration in numaflow-controller-config. It accepts a
YAML format configuration, it may include 4 sections, “stream”,
“consumer”, “otBucket” and “procBucket”. Available fields under “stream”
include “retention” (e.g. interest, limits, workerQueue), “maxMsgs”,
“maxAge” (e.g. 72h), “replicas” (1, 3, 5), “duplicates” (e.g. 5m).
Available fields under “consumer” include “ackWait” (e.g. 60s) Available
fields under “otBucket” include “maxValueSize”, “history”, “ttl”
(e.g. 72h), “maxBytes”, “replicas” (1, 3, 5). Available fields under
“procBucket” include “maxValueSize”, “history”, “ttl” (e.g. 72h),
“maxBytes”, “replicas” (1, 3, 5).
</p>
</td>
</tr>
<tr>
<td>
<code>encryption</code></br> <em> bool </em>
</td>
<td>
<em>(Optional)</em>
<p>
Whether encrypt the data at rest, defaults to false Enabling encryption
might impact the performance, see
<a href="https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/encryption_at_rest">https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/encryption_at_rest</a>
for the detail Toggling the value will impact encypting/decrypting
existing messages.
</p>
</td>
</tr>
<tr>
<td>
<code>tls</code></br> <em> bool </em>
</td>
<td>
<em>(Optional)</em>
<p>
Whether enable TLS, defaults to false Enabling TLS might impact the
performance
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.JetStreamConfig">
JetStreamConfig
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.BufferServiceConfig">BufferServiceConfig</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>url</code></br> <em> string </em>
</td>
<td>
<p>
JetStream (NATS) URL
</p>
</td>
</tr>
<tr>
<td>
<code>auth</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.NATSAuth"> NATSAuth </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>bufferConfig</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>tlsEnabled</code></br> <em> bool </em>
</td>
<td>
<p>
TLS enabled or not
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.KafkaSink">
KafkaSink
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Sink">Sink</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>brokers</code></br> <em> \[\]string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>topic</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>tls</code></br> <em> <a href="#numaflow.numaproj.io/v1alpha1.TLS">
TLS </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
TLS user to configure TLS connection for kafka broker TLS.enable=true
default for TLS.
</p>
</td>
</tr>
<tr>
<td>
<code>config</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.KafkaSource">
KafkaSource
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Source">Source</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>brokers</code></br> <em> \[\]string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>topic</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>consumerGroup</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>tls</code></br> <em> <a href="#numaflow.numaproj.io/v1alpha1.TLS">
TLS </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
TLS user to configure TLS connection for kafka broker TLS.enable=true
default for TLS.
</p>
</td>
</tr>
<tr>
<td>
<code>config</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Lifecycle">
Lifecycle
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec">PipelineSpec</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>deleteGracePeriodSeconds</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
DeleteGracePeriodSeconds used to delete pipeline gracefully
</p>
</td>
</tr>
<tr>
<td>
<code>desiredPhase</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelinePhase"> PipelinePhase
</a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
DesiredPhase used to bring the pipeline from current phase to desired
phase
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Log">
Log
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Sink">Sink</a>)
</p>
<p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.Metadata">
Metadata
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.DaemonTemplate">DaemonTemplate</a>,
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamBufferService">JetStreamBufferService</a>,
<a href="#numaflow.numaproj.io/v1alpha1.NativeRedis">NativeRedis</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>annotations</code></br> <em> map\[string\]string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>labels</code></br> <em> map\[string\]string </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.NATSAuth">
NATSAuth
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamConfig">JetStreamConfig</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>user</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Secret for auth user
</p>
</td>
</tr>
<tr>
<td>
<code>password</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Secret for auth password
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.NativeRedis">
NativeRedis
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisBufferService">RedisBufferService</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>version</code></br> <em> string </em>
</td>
<td>
<p>
Redis version, such as “6.0.16”
</p>
</td>
</tr>
<tr>
<td>
<code>replicas</code></br> <em> int32 </em>
</td>
<td>
<p>
Redis StatefulSet size
</p>
</td>
</tr>
<tr>
<td>
<code>redisContainerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
RedisContainerTemplate contains customized spec for Redis container
</p>
</td>
</tr>
<tr>
<td>
<code>sentinelContainerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
SentinelContainerTemplate contains customized spec for Redis container
</p>
</td>
</tr>
<tr>
<td>
<code>metricsContainerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
MetricsContainerTemplate contains customized spec for metrics container
</p>
</td>
</tr>
<tr>
<td>
<code>persistence</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PersistenceStrategy">
PersistenceStrategy </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>metadata</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Metadata"> Metadata </a> </em>
</td>
<td>
<p>
Metadata sets the pods’s metadata, i.e. annotations and labels
</p>
</td>
</tr>
<tr>
<td>
<code>nodeSelector</code></br> <em> map\[string\]string </em>
</td>
<td>
<em>(Optional)</em>
<p>
NodeSelector is a selector which must be true for the pod to fit on a
node. Selector which must match a node’s labels for the pod to be
scheduled on that node. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/assign-pod-node/">https://kubernetes.io/docs/concepts/configuration/assign-pod-node/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>tolerations</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#toleration-v1-core">
\[\]Kubernetes core/v1.Toleration </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
If specified, the pod’s tolerations.
</p>
</td>
</tr>
<tr>
<td>
<code>securityContext</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#podsecuritycontext-v1-core">
Kubernetes core/v1.PodSecurityContext </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
SecurityContext holds pod-level security attributes and common container
settings. Optional: Defaults to empty. See type description for default
values of each field.
</p>
</td>
</tr>
<tr>
<td>
<code>imagePullSecrets</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#localobjectreference-v1-core">
\[\]Kubernetes core/v1.LocalObjectReference </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
ImagePullSecrets is an optional list of references to secrets in the
same namespace to use for pulling any of the images used by this
PodSpec. If specified, these secrets will be passed to individual puller
implementations for them to use. For example, in the case of docker,
only DockerConfig type secrets are honored. More info:
<a href="https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod">https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod</a>
</p>
</td>
</tr>
<tr>
<td>
<code>priorityClassName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
If specified, indicates the Redis pod’s priority. “system-node-critical”
and “system-cluster-critical” are two special keywords which indicate
the highest priorities with the former being the highest priority. Any
other name must be defined by creating a PriorityClass object with that
name. If not specified, the pod priority will be default or zero if
there is no default. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/">https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>priority</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
The priority value. Various system components use this field to find the
priority of the Redis pod. When Priority Admission Controller is
enabled, it prevents users from setting this field. The admission
controller populates this field from PriorityClassName. The higher the
value, the higher the priority. More info:
<a href="https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/">https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>affinity</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#affinity-v1-core">
Kubernetes core/v1.Affinity </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
The pod’s scheduling constraints More info:
<a href="https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/">https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/</a>
</p>
</td>
</tr>
<tr>
<td>
<code>serviceAccountName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
ServiceAccountName to apply to the StatefulSet
</p>
</td>
</tr>
<tr>
<td>
<code>settings</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisSettings"> RedisSettings
</a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Redis configuration, if not specified, global settings in
numaflow-controller-config will be used.
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.PBQStorage">
PBQStorage
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.GroupBy">GroupBy</a>)
</p>
<p>
<p>
PBQStorage defines the persistence configuration for a vertex.
</p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>persistentVolumeClaim</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PersistenceStrategy">
PersistenceStrategy </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.PersistenceStrategy">
PersistenceStrategy
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamBufferService">JetStreamBufferService</a>,
<a href="#numaflow.numaproj.io/v1alpha1.NativeRedis">NativeRedis</a>,
<a href="#numaflow.numaproj.io/v1alpha1.PBQStorage">PBQStorage</a>)
</p>
<p>
<p>
PersistenceStrategy defines the strategy of persistence
</p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>storageClassName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Name of the StorageClass required by the claim. More info:
<a href="https://kubernetes.io/docs/concepts/storage/persistent-volumes#class-1">https://kubernetes.io/docs/concepts/storage/persistent-volumes#class-1</a>
</p>
</td>
</tr>
<tr>
<td>
<code>accessMode</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#persistentvolumeaccessmode-v1-core">
Kubernetes core/v1.PersistentVolumeAccessMode </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Available access modes such as ReadWriteOnce, ReadWriteMany
<a href="https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes">https://kubernetes.io/docs/concepts/storage/persistent-volumes/#access-modes</a>
</p>
</td>
</tr>
<tr>
<td>
<code>volumeSize</code></br> <em>
k8s.io/apimachinery/pkg/api/resource.Quantity </em>
</td>
<td>
<p>
Volume size, e.g. 50Gi
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Pipeline">
Pipeline
</h3>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>metadata</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#objectmeta-v1-meta">
Kubernetes meta/v1.ObjectMeta </a> </em>
</td>
<td>
Refer to the Kubernetes API documentation for the fields of the
<code>metadata</code> field.
</td>
</tr>
<tr>
<td>
<code>spec</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec"> PipelineSpec </a>
</em>
</td>
<td>
<br/> <br/>
<table>
<tr>
<td>
<code>interStepBufferServiceName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>vertices</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">
\[\]AbstractVertex </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>edges</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge"> \[\]Edge </a> </em>
</td>
<td>
<p>
Edges define the relationships between vertices
</p>
</td>
</tr>
<tr>
<td>
<code>lifecycle</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Lifecycle"> Lifecycle </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Lifecycle define the Lifecycle properties
</p>
</td>
</tr>
<tr>
<td>
<code>limits</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineLimits"> PipelineLimits
</a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Limits define the limitations such as buffer read batch size for all the
vertices of a pipeline, they could be overridden by each vertex’s
settings
</p>
</td>
</tr>
<tr>
<td>
<code>watermark</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Watermark"> Watermark </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Watermark enables watermark progression across the entire pipeline.
Updating this after the pipeline has been created will have no impact
and will be ignored. To make the pipeline honor any changes to the
setting, the pipeline should be recreated.
</p>
</td>
</tr>
<tr>
<td>
<code>templates</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Templates"> Templates </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Templates is used to customize additional kubernetes resources required
for the Pipeline
</p>
</td>
</tr>
</table>
</td>
</tr>
<tr>
<td>
<code>status</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineStatus"> PipelineStatus
</a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.PipelineLimits">
PipelineLimits
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec">PipelineSpec</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>readBatchSize</code></br> <em> uint64 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Read batch size for all the vertices in the pipeline, can be overridden
by the vertex’s limit settings
</p>
</td>
</tr>
<tr>
<td>
<code>bufferMaxLength</code></br> <em> uint64 </em>
</td>
<td>
<em>(Optional)</em>
<p>
BufferMaxLength is used to define the max length of a buffer Only
applies to UDF and Source vertice as only they do buffer write. It can
be overridden by the settings in vertex limits.
</p>
</td>
</tr>
<tr>
<td>
<code>bufferUsageLimit</code></br> <em> uint32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
BufferUsageLimit is used to define the percentage of the buffer usage
limit, a valid value should be less than 100, for example, 85. Only
applies to UDF and Source vertice as only they do buffer write. It will
be overridden by the settings in vertex limits.
</p>
</td>
</tr>
<tr>
<td>
<code>readTimeout</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Read timeout for all the vertices in the pipeline, can be overridden by
the vertex’s limit settings
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.PipelinePhase">
PipelinePhase (<code>string</code> alias)
</p>
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Lifecycle">Lifecycle</a>,
<a href="#numaflow.numaproj.io/v1alpha1.PipelineStatus">PipelineStatus</a>)
</p>
<p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.PipelineSpec">
PipelineSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Pipeline">Pipeline</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>interStepBufferServiceName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>vertices</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">
\[\]AbstractVertex </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>edges</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge"> \[\]Edge </a> </em>
</td>
<td>
<p>
Edges define the relationships between vertices
</p>
</td>
</tr>
<tr>
<td>
<code>lifecycle</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Lifecycle"> Lifecycle </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Lifecycle define the Lifecycle properties
</p>
</td>
</tr>
<tr>
<td>
<code>limits</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineLimits"> PipelineLimits
</a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Limits define the limitations such as buffer read batch size for all the
vertices of a pipeline, they could be overridden by each vertex’s
settings
</p>
</td>
</tr>
<tr>
<td>
<code>watermark</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Watermark"> Watermark </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Watermark enables watermark progression across the entire pipeline.
Updating this after the pipeline has been created will have no impact
and will be ignored. To make the pipeline honor any changes to the
setting, the pipeline should be recreated.
</p>
</td>
</tr>
<tr>
<td>
<code>templates</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Templates"> Templates </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Templates is used to customize additional kubernetes resources required
for the Pipeline
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.PipelineStatus">
PipelineStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Pipeline">Pipeline</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>Status</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Status"> Status </a> </em>
</td>
<td>
<p>
(Members of <code>Status</code> are embedded into this type.)
</p>
</td>
</tr>
<tr>
<td>
<code>phase</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelinePhase"> PipelinePhase
</a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>message</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>lastUpdated</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#time-v1-meta">
Kubernetes meta/v1.Time </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>vertexCount</code></br> <em> uint32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>sourceCount</code></br> <em> uint32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>sinkCount</code></br> <em> uint32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>udfCount</code></br> <em> uint32 </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.RedisBufferService">
RedisBufferService
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferServiceSpec">InterStepBufferServiceSpec</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>native</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.NativeRedis"> NativeRedis </a>
</em>
</td>
<td>
<p>
Native brings up a native Redis service
</p>
</td>
</tr>
<tr>
<td>
<code>external</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisConfig"> RedisConfig </a>
</em>
</td>
<td>
<p>
External holds an External Redis config
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.RedisConfig">
RedisConfig
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.BufferServiceConfig">BufferServiceConfig</a>,
<a href="#numaflow.numaproj.io/v1alpha1.RedisBufferService">RedisBufferService</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>url</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Redis URL
</p>
</td>
</tr>
<tr>
<td>
<code>sentinelUrl</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Sentinel URL, will be ignored if Redis URL is provided
</p>
</td>
</tr>
<tr>
<td>
<code>masterName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Only required when Sentinel is used
</p>
</td>
</tr>
<tr>
<td>
<code>user</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Redis user
</p>
</td>
</tr>
<tr>
<td>
<code>password</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Redis password secret selector
</p>
</td>
</tr>
<tr>
<td>
<code>sentinelPassword</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Sentinel password secret selector
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.RedisSettings">
RedisSettings
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.NativeRedis">NativeRedis</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>redis</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Redis settings shared by both master and slaves, will override the
global settings from controller config
</p>
</td>
</tr>
<tr>
<td>
<code>master</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Special settings for Redis master node, will override the global
settings from controller config
</p>
</td>
</tr>
<tr>
<td>
<code>replica</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Special settings for Redis replica nodes, will override the global
settings from controller config
</p>
</td>
</tr>
<tr>
<td>
<code>sentinel</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Sentinel settings, will override the global settings from controller
config
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Scale">
Scale
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>)
</p>
<p>
<p>
Scale defines the parameters for autoscaling.
</p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>disabled</code></br> <em> bool </em>
</td>
<td>
<em>(Optional)</em>
<p>
Whether to disable autoscaling. Set to “true” when using Kubernetes HPA
or any other 3rd party autoscaling strategies.
</p>
</td>
</tr>
<tr>
<td>
<code>min</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Minimum replicas.
</p>
</td>
</tr>
<tr>
<td>
<code>max</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Maximum replicas.
</p>
</td>
</tr>
<tr>
<td>
<code>lookbackSeconds</code></br> <em> uint32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Lookback seconds to calculate the average pending messages and
processing rate.
</p>
</td>
</tr>
<tr>
<td>
<code>cooldownSeconds</code></br> <em> uint32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Cooldown seconds after a scaling operation before another one.
</p>
</td>
</tr>
<tr>
<td>
<code>zeroReplicaSleepSeconds</code></br> <em> uint32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
After scaling down to 0, sleep how many seconds before scaling up to
peek.
</p>
</td>
</tr>
<tr>
<td>
<code>targetProcessingSeconds</code></br> <em> uint32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
TargetProcessingSeconds is used to tune the aggressiveness of
autoscaling for source vertices, it measures how fast you want the
vertex to process all the pending messages. Typically increasing the
value, which leads to lower processing rate, thus less replicas. It’s
only effective for source vertices.
</p>
</td>
</tr>
<tr>
<td>
<code>targetBufferUsage</code></br> <em> uint32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
TargetBufferUsage is used to define the target percentage of usage of
the buffer to be read. A valid and meaningful value should be less than
the BufferUsageLimit defined in the Edge spec (or Pipeline spec), for
example, 50. It only applies to UDF and Sink vertices as only they have
buffers to read.
</p>
</td>
</tr>
<tr>
<td>
<code>replicasPerScale</code></br> <em> uint32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
ReplicasPerScale defines maximum replicas can be scaled up or down at
once. The is use to prevent too aggressive scaling operations
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Sink">
Sink
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>log</code></br> <em> <a href="#numaflow.numaproj.io/v1alpha1.Log">
Log </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>kafka</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.KafkaSink"> KafkaSink </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>udsink</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.UDSink"> UDSink </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Source">
Source
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>generator</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.GeneratorSource">
GeneratorSource </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>kafka</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.KafkaSource"> KafkaSource </a>
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>http</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.HTTPSource"> HTTPSource </a>
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Status">
Status
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferServiceStatus">InterStepBufferServiceStatus</a>,
<a href="#numaflow.numaproj.io/v1alpha1.PipelineStatus">PipelineStatus</a>)
</p>
<p>
<p>
Status is a common structure which can be used for Status field.
</p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>conditions</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#condition-v1-meta">
\[\]Kubernetes meta/v1.Condition </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Conditions are the latest available observations of a resource’s current
state.
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.StoreType">
StoreType (<code>string</code> alias)
</p>
</h3>
<p>
<p>
PBQ store’s backend type.
</p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.TLS">
TLS
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.KafkaSink">KafkaSink</a>,
<a href="#numaflow.numaproj.io/v1alpha1.KafkaSource">KafkaSource</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>insecureSkipVerify</code></br> <em> bool </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>caCertSecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
CACertSecret refers to the secret that contains the CA cert
</p>
</td>
</tr>
<tr>
<td>
<code>clientCertSecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
CertSecret refers to the secret that contains the cert
</p>
</td>
</tr>
<tr>
<td>
<code>clientKeySecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
KeySecret refers to the secret that contains the key
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Templates">
Templates
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec">PipelineSpec</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>daemon</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.DaemonTemplate"> DaemonTemplate
</a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
DaemonTemplate is used to customize the Daemon Deployment
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.UDF">
UDF
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>container</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Container"> Container </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>builtin</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Function"> Function </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>groupBy</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.GroupBy"> GroupBy </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.UDSink">
UDSink
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Sink">Sink</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>container</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Container"> Container </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Vertex">
Vertex
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexInstance">VertexInstance</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>metadata</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#objectmeta-v1-meta">
Kubernetes meta/v1.ObjectMeta </a> </em>
</td>
<td>
Refer to the Kubernetes API documentation for the fields of the
<code>metadata</code> field.
</td>
</tr>
<tr>
<td>
<code>spec</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexSpec"> VertexSpec </a>
</em>
</td>
<td>
<br/> <br/>
<table>
<tr>
<td>
<code>AbstractVertex</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex"> AbstractVertex
</a> </em>
</td>
<td>
<p>
(Members of <code>AbstractVertex</code> are embedded into this type.)
</p>
</td>
</tr>
<tr>
<td>
<code>pipelineName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>interStepBufferServiceName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>replicas</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>fromEdges</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge"> \[\]Edge </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>toEdges</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge"> \[\]Edge </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</table>
</td>
</tr>
<tr>
<td>
<code>status</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexStatus"> VertexStatus </a>
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.VertexInstance">
VertexInstance
</h3>
<p>
<p>
VertexInstance is a wrapper of a vertex instance, which contains the
vertex spec and the instance information such as hostname and replica
index.
</p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>vertex</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Vertex"> Vertex </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>hostname</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>replica</code></br> <em> int32 </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.VertexLimits">
VertexLimits
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>readBatchSize</code></br> <em> uint64 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Read batch size from the source or buffer. It overrides the settings
from pipeline limits.
</p>
</td>
</tr>
<tr>
<td>
<code>readTimeout</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Read timeout duration from the source or buffer It overrides the
settings from pipeline limits.
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.VertexPhase">
VertexPhase (<code>string</code> alias)
</p>
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexStatus">VertexStatus</a>)
</p>
<p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.VertexSpec">
VertexSpec
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Vertex">Vertex</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>AbstractVertex</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex"> AbstractVertex
</a> </em>
</td>
<td>
<p>
(Members of <code>AbstractVertex</code> are embedded into this type.)
</p>
</td>
</tr>
<tr>
<td>
<code>pipelineName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>interStepBufferServiceName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>replicas</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>fromEdges</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge"> \[\]Edge </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>toEdges</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge"> \[\]Edge </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.VertexStatus">
VertexStatus
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Vertex">Vertex</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>phase</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexPhase"> VertexPhase </a>
</em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>reason</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>message</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>replicas</code></br> <em> uint32 </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>selector</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>lastScaledAt</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#time-v1-meta">
Kubernetes meta/v1.Time </a> </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.VertexType">
VertexType (<code>string</code> alias)
</p>
</h3>
<p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.Watermark">
Watermark
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec">PipelineSpec</a>)
</p>
<p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>disabled</code></br> <em> bool </em>
</td>
<td>
<em>(Optional)</em>
<p>
Disabled toggles the watermark propagation, defaults to false.
</p>
</td>
</tr>
<tr>
<td>
<code>maxDelay</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Maximum delay allowed for watermark calculation, defaults to “0s”, which
means no delay.
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Window">
Window
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.GroupBy">GroupBy</a>)
</p>
<p>
<p>
Window describes windowing strategy
</p>
</p>
<table>
<thead>
<tr>
<th>
Field
</th>
<th>
Description
</th>
</tr>
</thead>
<tbody>
<tr>
<td>
<code>fixed</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.FixedWindow"> FixedWindow </a>
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.WindowType">
WindowType (<code>string</code> alias)
</p>
</h3>
<p>
</p>
<hr/>
<p>
<em> Generated with <code>gen-crd-api-reference-docs</code>. </em>
</p>
