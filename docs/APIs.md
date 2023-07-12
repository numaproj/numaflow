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
<h3 id="numaflow.numaproj.io/v1alpha1.AbstractPodTemplate">
AbstractPodTemplate
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.DaemonTemplate">DaemonTemplate</a>,
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamBufferService">JetStreamBufferService</a>,
<a href="#numaflow.numaproj.io/v1alpha1.JobTemplate">JobTemplate</a>,
<a href="#numaflow.numaproj.io/v1alpha1.NativeRedis">NativeRedis</a>)
</p>
<p>
<p>
AbstractPodTemplate provides a template for pod customization in
vertices, daemon deployments and so on.
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
ServiceAccountName applied to the pod
</p>
</td>
</tr>
<tr>
<td>
<code>runtimeClassName</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
RuntimeClassName refers to a RuntimeClass object in the node.k8s.io
group, which should be used to run this pod. If no RuntimeClass resource
matches the named class, the pod will not be run. If unset or empty, the
“legacy” RuntimeClass will be used, which is an implicit class with an
empty definition that uses the default runtime handler. More info:
<a href="https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class">https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class</a>
</p>
</td>
</tr>
<tr>
<td>
<code>automountServiceAccountToken</code></br> <em> bool </em>
</td>
<td>
<em>(Optional)</em>
<p>
AutomountServiceAccountToken indicates whether a service account token
should be automatically mounted.
</p>
</td>
</tr>
<tr>
<td>
<code>dnsPolicy</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#dnspolicy-v1-core">
Kubernetes core/v1.DNSPolicy </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Set DNS policy for the pod. Defaults to “ClusterFirst”. Valid values are
‘ClusterFirstWithHostNet’, ‘ClusterFirst’, ‘Default’ or ‘None’. DNS
parameters given in DNSConfig will be merged with the policy selected
with DNSPolicy. To have DNS options set along with hostNetwork, you have
to specify DNS policy explicitly to ‘ClusterFirstWithHostNet’.
</p>
</td>
</tr>
<tr>
<td>
<code>dnsConfig</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#poddnsconfig-v1-core">
Kubernetes core/v1.PodDNSConfig </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Specifies the DNS parameters of a pod. Parameters specified here will be
merged to the generated DNS configuration based on DNSPolicy.
</p>
</td>
</tr>
</tbody>
</table>
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
<code>udf</code></br> <em> <a href="#numaflow.numaproj.io/v1alpha1.UDF">
UDF </a> </em>
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
<code>initContainerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>AbstractPodTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractPodTemplate">
AbstractPodTemplate </a> </em>
</td>
<td>
<p>
(Members of <code>AbstractPodTemplate</code> are embedded into this
type.)
</p>
<em>(Optional)</em>
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
<tr>
<td>
<code>sidecars</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#container-v1-core">
\[\]Kubernetes core/v1.Container </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
List of sidecar containers belonging to the pod.
</p>
</td>
</tr>
<tr>
<td>
<code>partitions</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Number of partitions of the vertex owned buffers. It applies to udf and
sink vertices only.
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
<h3 id="numaflow.numaproj.io/v1alpha1.BasicAuth">
BasicAuth
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.NatsAuth">NatsAuth</a>)
</p>
<p>
<p>
BasicAuth represents the basic authentication approach which contains a
user name and a password.
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
<h3 id="numaflow.numaproj.io/v1alpha1.Blackhole">
Blackhole
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Sink">Sink</a>)
</p>
<p>
<p>
Blackhole is a sink to emulate /dev/null
</p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.BufferFullWritingStrategy">
BufferFullWritingStrategy (<code>string</code> alias)
</p>
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge">Edge</a>)
</p>
<p>
</p>
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
<h3 id="numaflow.numaproj.io/v1alpha1.CombinedEdge">
CombinedEdge
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexSpec">VertexSpec</a>)
</p>
<p>
<p>
CombinedEdge is a combination of Edge and some other properties such as
vertex type, partitions, limits. It’s used to decorate the fromEdges and
toEdges of the generated Vertex objects, so that in the vertex pod, it
knows the properties of the connected vertices, for example, how many
partitioned buffers I should write to, what is the write buffer length,
etc.
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
<code>Edge</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Edge"> Edge </a> </em>
</td>
<td>
<p>
(Members of <code>Edge</code> are embedded into this type.)
</p>
</td>
</tr>
<tr>
<td>
<code>fromVertexType</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexType"> VertexType </a>
</em>
</td>
<td>
<p>
From vertex type.
</p>
</td>
</tr>
<tr>
<td>
<code>fromVertexPartitionCount</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
The number of partitions of the from vertex, if not provided, the
default value is set to “1”.
</p>
</td>
</tr>
<tr>
<td>
<code>fromVertexLimits</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexLimits"> VertexLimits </a>
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>toVertexType</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexType"> VertexType </a>
</em>
</td>
<td>
<p>
To vertex type.
</p>
</td>
</tr>
<tr>
<td>
<code>toVertexPartitionCount</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
The number of partitions of the to vertex, if not provided, the default
value is set to “1”.
</p>
</td>
</tr>
<tr>
<td>
<code>toVertexLimits</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexLimits"> VertexLimits </a>
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
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
<a href="#numaflow.numaproj.io/v1alpha1.UDSink">UDSink</a>,
<a href="#numaflow.numaproj.io/v1alpha1.UDTransformer">UDTransformer</a>)
</p>
<p>
<p>
Container is used to define the container properties for user defined
functions, sinks, etc.
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
<code>envFrom</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#envfromsource-v1-core">
\[\]Kubernetes core/v1.EnvFromSource </a> </em>
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
<tr>
<td>
<code>securityContext</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#securitycontext-v1-core">
Kubernetes core/v1.SecurityContext </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>imagePullPolicy</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#pullpolicy-v1-core">
Kubernetes core/v1.PullPolicy </a> </em>
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
<a href="#numaflow.numaproj.io/v1alpha1.JobTemplate">JobTemplate</a>,
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
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>imagePullPolicy</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#pullpolicy-v1-core">
Kubernetes core/v1.PullPolicy </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>securityContext</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#securitycontext-v1-core">
Kubernetes core/v1.SecurityContext </a> </em>
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
<code>envFrom</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#envfromsource-v1-core">
\[\]Kubernetes core/v1.EnvFromSource </a> </em>
</td>
<td>
<em>(Optional)</em>
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
<code>AbstractPodTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractPodTemplate">
AbstractPodTemplate </a> </em>
</td>
<td>
<p>
(Members of <code>AbstractPodTemplate</code> are embedded into this
type.)
</p>
<em>(Optional)</em>
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
<code>initContainerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Edge">
Edge
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.CombinedEdge">CombinedEdge</a>,
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
<code>onFull</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.BufferFullWritingStrategy">
BufferFullWritingStrategy </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
OnFull specifies the behaviour for the write actions when the inter step
buffer is full. There are currently two options, retryUntilSuccess and
discardLatest. if not provided, the default value is set to
“retryUntilSuccess”
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
<code>tags</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.TagConditions"> TagConditions
</a> </em>
</td>
<td>
<p>
Tags used to specify tags for conditional forwarding
</p>
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
<h3 id="numaflow.numaproj.io/v1alpha1.GSSAPI">
GSSAPI
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.SASL">SASL</a>)
</p>
<p>
<p>
GSSAPI represents a SASL GSSAPI config
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
<code>serviceName</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>realm</code></br> <em> string </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>usernameSecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<p>
UsernameSecret refers to the secret that contains the username
</p>
</td>
</tr>
<tr>
<td>
<code>authType</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.KRB5AuthType"> KRB5AuthType </a>
</em>
</td>
<td>
<p>
valid inputs - KRB5_USER_AUTH, KRB5_KEYTAB_AUTH
</p>
</td>
</tr>
<tr>
<td>
<code>passwordSecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
PasswordSecret refers to the secret that contains the password
</p>
</td>
</tr>
<tr>
<td>
<code>keytabSecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
KeytabSecret refers to the secret that contains the keytab
</p>
</td>
</tr>
<tr>
<td>
<code>kerberosConfigSecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
KerberosConfigSecret refers to the secret that contains the kerberos
config
</p>
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
<tr>
<td>
<code>keyCount</code></br> <em> int32 </em>
</td>
<td>
<p>
KeyCount is the number of unique keys in the payload
</p>
</td>
</tr>
<tr>
<td>
<code>value</code></br> <em> uint64 </em>
</td>
<td>
<p>
Value is an optional uint64 value to be written in to the payload
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
<code>allowedLateness</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
AllowedLateness allows late data to be included for the Reduce operation
as long as the late data is not later than (Watermark -
AllowedLateness).
</p>
</td>
</tr>
<tr>
<td>
<code>storage</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PBQStorage"> PBQStorage </a>
</em>
</td>
<td>
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
<code>AbstractPodTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractPodTemplate">
AbstractPodTemplate </a> </em>
</td>
<td>
<p>
(Members of <code>AbstractPodTemplate</code> are embedded into this
type.)
</p>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>settings</code></br> <em> string </em>
</td>
<td>
<em>(Optional)</em>
<p>
Nats/JetStream configuration, if not specified, global settings in
numaflow-controller-config will be used. See
<a href="https://docs.nats.io/running-a-nats-service/configuration#limits">https://docs.nats.io/running-a-nats-service/configuration#limits</a>
and
<a href="https://docs.nats.io/running-a-nats-service/configuration#jetstream">https://docs.nats.io/running-a-nats-service/configuration#jetstream</a>.
For limits, only “max_payload” is supported for configuration, defaults
to 1048576 (1MB), not recommended to use values over 8388608 (8MB) but
max_payload can be set up to 67108864 (64MB). For jetstream, only
“max_memory_store” and “max_file_store” are supported for configuration,
do not set “store_dir” as it has been hardcoded.
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
for the detail Toggling the value will impact encrypting/decrypting
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
<a href="#numaflow.numaproj.io/v1alpha1.NatsAuth"> NatsAuth </a> </em>
</td>
<td>
</td>
</tr>
<tr>
<td>
<code>streamConfig</code></br> <em> string </em>
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
<h3 id="numaflow.numaproj.io/v1alpha1.JobTemplate">
JobTemplate
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
<code>AbstractPodTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractPodTemplate">
AbstractPodTemplate </a> </em>
</td>
<td>
<p>
(Members of <code>AbstractPodTemplate</code> are embedded into this
type.)
</p>
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
<code>ttlSecondsAfterFinished</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
ttlSecondsAfterFinished limits the lifetime of a Job that has finished
execution (either Complete or Failed). If this field is set,
ttlSecondsAfterFinished after the Job finishes, it is eligible to be
automatically deleted. When the Job is being deleted, its lifecycle
guarantees (e.g. finalizers) will be honored. If this field is unset,
the Job won’t be automatically deleted. If this field is set to zero,
the Job becomes eligible to be deleted immediately after it finishes.
Numaflow defaults to 30
</p>
</td>
</tr>
<tr>
<td>
<code>backoffLimit</code></br> <em> int32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
Specifies the number of retries before marking this job failed. More
info:
<a href="https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-backoff-failure-policy">https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-backoff-failure-policy</a>
Numaflow defaults to 20
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.KRB5AuthType">
KRB5AuthType (<code>string</code> alias)
</p>
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.GSSAPI">GSSAPI</a>)
</p>
<p>
<p>
KRB5AuthType describes the kerberos auth type
</p>
</p>
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
<tr>
<td>
<code>sasl</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SASL"> SASL </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
SASL user to configure SASL connection for kafka broker SASL.enable=true
default for SASL.
</p>
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
<tr>
<td>
<code>sasl</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SASL"> SASL </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
SASL user to configure SASL connection for kafka broker SASL.enable=true
default for SASL.
</p>
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
<h3 id="numaflow.numaproj.io/v1alpha1.LogicOperator">
LogicOperator (<code>string</code> alias)
</p>
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.TagConditions">TagConditions</a>)
</p>
<p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.Metadata">
Metadata
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractPodTemplate">AbstractPodTemplate</a>)
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
<code>initContainerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>
<td>
<em>(Optional)</em>
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
<code>AbstractPodTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractPodTemplate">
AbstractPodTemplate </a> </em>
</td>
<td>
<p>
(Members of <code>AbstractPodTemplate</code> are embedded into this
type.)
</p>
<em>(Optional)</em>
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
<h3 id="numaflow.numaproj.io/v1alpha1.NatsAuth">
NatsAuth
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamConfig">JetStreamConfig</a>,
<a href="#numaflow.numaproj.io/v1alpha1.NatsSource">NatsSource</a>)
</p>
<p>
<p>
NatsAuth defines how to authenticate the nats access
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
<code>basic</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.BasicAuth"> BasicAuth </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Basic auth which contains a user name and a password
</p>
</td>
</tr>
<tr>
<td>
<code>token</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Token auth
</p>
</td>
</tr>
<tr>
<td>
<code>nkey</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
NKey auth
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.NatsSource">
NatsSource
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
<code>url</code></br> <em> string </em>
</td>
<td>
<p>
URL to connect to NATS cluster, multiple urls could be separated by
comma.
</p>
</td>
</tr>
<tr>
<td>
<code>subject</code></br> <em> string </em>
</td>
<td>
<p>
Subject holds the name of the subject onto which messages are published.
</p>
</td>
</tr>
<tr>
<td>
<code>queue</code></br> <em> string </em>
</td>
<td>
<p>
Queue is used for queue subscription.
</p>
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
TLS configuration for the nats client.
</p>
</td>
</tr>
<tr>
<td>
<code>auth</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.NatsAuth"> NatsAuth </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Auth information
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
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>emptyDir</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#emptydirvolumesource-v1-core">
Kubernetes core/v1.EmptyDirVolumeSource </a> </em>
</td>
<td>
<em>(Optional)</em>
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
applies to UDF and Source vertices as only they do buffer write. It can
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
applies to UDF and Source vertices as only they do buffer write. It will
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
<a href="#numaflow.numaproj.io/v1alpha1.RedisBufferService">RedisBufferService</a>,
<a href="#numaflow.numaproj.io/v1alpha1.RedisStreamsSource">RedisStreamsSource</a>)
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
<h3 id="numaflow.numaproj.io/v1alpha1.RedisStreamsSource">
RedisStreamsSource
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
<code>RedisConfig</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisConfig"> RedisConfig </a>
</em>
</td>
<td>
<p>
(Members of <code>RedisConfig</code> are embedded into this type.)
</p>
<p>
RedisConfig contains connectivity info
</p>
</td>
</tr>
<tr>
<td>
<code>stream</code></br> <em> string </em>
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
<code>readFromBeginning</code></br> <em> bool </em>
</td>
<td>
<p>
if true, stream starts being read from the beginning; otherwise, the
latest
</p>
</td>
</tr>
<tr>
<td>
<code>tls</code></br> <em> <a href="#numaflow.numaproj.io/v1alpha1.TLS">
TLS </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.SASL">
SASL
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
<code>mechanism</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SASLType"> SASLType </a> </em>
</td>
<td>
<p>
SASL mechanism to use
</p>
</td>
</tr>
<tr>
<td>
<code>gssapi</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.GSSAPI"> GSSAPI </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
GSSAPI contains the kerberos config
</p>
</td>
</tr>
<tr>
<td>
<code>plain</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SASLPlain"> SASLPlain </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
SASLPlain contains the sasl plain config
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.SASLPlain">
SASLPlain
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.SASL">SASL</a>)
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
<code>userSecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<p>
UserSecret refers to the secret that contains the user
</p>
</td>
</tr>
<tr>
<td>
<code>passwordSecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
PasswordSecret refers to the secret that contains the password
</p>
</td>
</tr>
<tr>
<td>
<code>handshake</code></br> <em> bool </em>
</td>
<td>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.SASLType">
SASLType (<code>string</code> alias)
</p>
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.SASL">SASL</a>)
</p>
<p>
<p>
SASLType describes the SASL type
</p>
</p>
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
<code>targetBufferAvailability</code></br> <em> uint32 </em>
</td>
<td>
<em>(Optional)</em>
<p>
TargetBufferAvailability is used to define the target percentage of the
buffer availability. A valid and meaningful value should be less than
the BufferUsageLimit defined in the Edge spec (or Pipeline spec), for
example, 50. It only applies to UDF and Sink vertices because only they
have buffers to read.
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
<code>blackhole</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Blackhole"> Blackhole </a> </em>
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
<h3 id="numaflow.numaproj.io/v1alpha1.SlidingWindow">
SlidingWindow
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Window">Window</a>)
</p>
<p>
<p>
SlidingWindow describes a sliding window
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
<tr>
<td>
<code>slide</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
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
<tr>
<td>
<code>nats</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.NatsSource"> NatsSource </a>
</em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>redisStreams</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisStreamsSource">
RedisStreamsSource </a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>transformer</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.UDTransformer"> UDTransformer
</a> </em>
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
<h3 id="numaflow.numaproj.io/v1alpha1.TLS">
TLS
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.KafkaSink">KafkaSink</a>,
<a href="#numaflow.numaproj.io/v1alpha1.KafkaSource">KafkaSource</a>,
<a href="#numaflow.numaproj.io/v1alpha1.NatsSource">NatsSource</a>,
<a href="#numaflow.numaproj.io/v1alpha1.RedisStreamsSource">RedisStreamsSource</a>)
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
<h3 id="numaflow.numaproj.io/v1alpha1.TagConditions">
TagConditions
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.ForwardConditions">ForwardConditions</a>)
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
<code>operator</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.LogicOperator"> LogicOperator
</a> </em>
</td>
<td>
<em>(Optional)</em>
<p>
Operator specifies the type of operation that should be used for
conditional forwarding value could be “and”, “or”, “not”
</p>
</td>
</tr>
<tr>
<td>
<code>values</code></br> <em> \[\]string </em>
</td>
<td>
<p>
Values tag values for conditional forwarding
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
<tr>
<td>
<code>job</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.JobTemplate"> JobTemplate </a>
</em>
</td>
<td>
<em>(Optional)</em>
<p>
JobTemplate is used to customize Jobs
</p>
</td>
</tr>
</tbody>
</table>
<h3 id="numaflow.numaproj.io/v1alpha1.Transformer">
Transformer
</h3>
<p>
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.UDTransformer">UDTransformer</a>)
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
<h3 id="numaflow.numaproj.io/v1alpha1.UDTransformer">
UDTransformer
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
<a href="#numaflow.numaproj.io/v1alpha1.Transformer"> Transformer </a>
</em>
</td>
<td>
<em>(Optional)</em>
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
<a href="#numaflow.numaproj.io/v1alpha1.CombinedEdge"> \[\]CombinedEdge
</a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>toEdges</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.CombinedEdge"> \[\]CombinedEdge
</a> </em>
</td>
<td>
<em>(Optional)</em>
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
Watermark indicates watermark progression in the vertex, it’s populated
from the pipeline watermark settings.
</p>
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
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.CombinedEdge">CombinedEdge</a>)
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
<a href="#numaflow.numaproj.io/v1alpha1.CombinedEdge"> \[\]CombinedEdge
</a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
<tr>
<td>
<code>toEdges</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.CombinedEdge"> \[\]CombinedEdge
</a> </em>
</td>
<td>
<em>(Optional)</em>
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
Watermark indicates watermark progression in the vertex, it’s populated
from the pipeline watermark settings.
</p>
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
(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.CombinedEdge">CombinedEdge</a>)
</p>
<p>
</p>
<h3 id="numaflow.numaproj.io/v1alpha1.Watermark">
Watermark
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
<tr>
<td>
<code>sliding</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SlidingWindow"> SlidingWindow
</a> </em>
</td>
<td>
<em>(Optional)</em>
</td>
</tr>
</tbody>
</table>
<hr/>
<p>
<em> Generated with <code>gen-crd-api-reference-docs</code>. </em>
</p>
