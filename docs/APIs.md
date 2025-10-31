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

<h3 id="numaflow.numaproj.io/v1alpha1.AWSAssumeRole">

AWSAssumeRole
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.SqsSink">SqsSink</a>,
<a href="#numaflow.numaproj.io/v1alpha1.SqsSource">SqsSource</a>)
</p>

<p>

<p>

AWSAssumeRole contains the configuration for AWS STS assume role
authentication This can be used with any AWS service (SQS, S3, DynamoDB,
etc.)
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

<code>roleArn</code></br> <em> string </em>
</td>

<td>

<p>

RoleARN is the Amazon Resource Name (ARN) of the role to assume. This is
a required field when assume role is enabled. Example:
“arn:aws:iam::123456789012:role/CrossAccount-Service-Role”
</p>

</td>

</tr>

<tr>

<td>

<code>sessionName</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
<p>

SessionName is an identifier for the assumed role session. This appears
in AWS CloudTrail logs to help identify the source of API calls. If not
specified, a default session name will be generated based on the service
context.
</p>

</td>

</tr>

<tr>

<td>

<code>durationSeconds</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

DurationSeconds is the duration (in seconds) of the role session. Valid
values: 900-43200 (15 minutes to 12 hours) Defaults to 3600 (1 hour) if
not specified. The actual session duration is constrained by the maximum
session duration setting of the IAM role being assumed.
</p>

</td>

</tr>

<tr>

<td>

<code>externalID</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
<p>

ExternalID is a unique identifier that might be required when you assume
a role in another account. This is commonly used as an additional
security measure for cross-account role access.
</p>

</td>

</tr>

<tr>

<td>

<code>policy</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
<p>

Policy is an IAM policy document (JSON string) that you want to use as
an inline session policy. This parameter is optional. When specified,
the session permissions are the intersection of the IAM role’s
identity-based policy and the session policies. This allows further
restriction of permissions for the specific service operations.
</p>

</td>

</tr>

<tr>

<td>

<code>policyArns</code></br> <em> \[\]string </em>
</td>

<td>

<em>(Optional)</em>
<p>

PolicyARNs is a list of Amazon Resource Names (ARNs) of IAM managed
policies that you want to use as managed session policies. The policies
must exist in the same account as the role. This allows attaching
existing managed policies to further restrict session permissions.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.AbstractPodTemplate">

AbstractPodTemplate
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.DaemonTemplate">DaemonTemplate</a>,
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamBufferService">JetStreamBufferService</a>,
<a href="#numaflow.numaproj.io/v1alpha1.JobTemplate">JobTemplate</a>,
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>,
<a href="#numaflow.numaproj.io/v1alpha1.ServingSpec">ServingSpec</a>,
<a href="#numaflow.numaproj.io/v1alpha1.SideInputsManagerTemplate">SideInputsManagerTemplate</a>,
<a href="#numaflow.numaproj.io/v1alpha1.VertexTemplate">VertexTemplate</a>)
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

If specified, indicates the pod’s priority. “system-node-critical” and
“system-cluster-critical” are two special keywords which indicate the
highest priorities with the former being the highest priority. Any other
name must be defined by creating a PriorityClass object with that name.
If not specified, the pod priority will be default or zero if there is
no default. More info:
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
priority of the pod. When Priority Admission Controller is enabled, it
prevents users from setting this field. The admission controller
populates this field from PriorityClassName. The higher the value, the
higher the priority. More info:
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

<tr>

<td>

<code>resourceClaims</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#podresourceclaim-v1-core">
\[\]Kubernetes core/v1.PodResourceClaim </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

ResourceClaims defines which ResourceClaims must be allocated and
reserved before the Pod is allowed to start. The resources will be made
available to those containers which consume them by name.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.AbstractSink">

AbstractSink
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

<code>log</code></br> <em> <a href="#numaflow.numaproj.io/v1alpha1.Log">
Log </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Log sink is used to write the data to the log.
</p>

</td>

</tr>

<tr>

<td>

<code>kafka</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.KafkaSink"> KafkaSink </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Kafka sink is used to write the data to the Kafka.
</p>

</td>

</tr>

<tr>

<td>

<code>blackhole</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Blackhole"> Blackhole </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Blackhole sink is used to write the data to the blackhole sink, which is
a sink that discards all the data written to it.
</p>

</td>

</tr>

<tr>

<td>

<code>udsink</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.UDSink"> UDSink </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

UDSink sink is used to write the data to the user-defined sink.
</p>

</td>

</tr>

<tr>

<td>

<code>serve</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ServeSink"> ServeSink </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Serve sink is used to return results when working with a
ServingPipeline.
</p>

</td>

</tr>

<tr>

<td>

<code>sqs</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SqsSink"> SqsSink </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

SQS sink is used to write the data to the AWS SQS.
</p>

</td>

</tr>

<tr>

<td>

<code>pulsar</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PulsarSink"> PulsarSink </a>
</em>
</td>

<td>

<em>(Optional)</em>
<p>

Pulsar sink is used to write the data to the Apache Pulsar.
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
<p>

Container template for the main numa container.
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
<p>

Container template for all the vertex pod init containers spawned by
numaflow, excluding the ones specified by the user.
</p>

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

List of customized init containers belonging to the pod. More info:
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

List of customized sidecar containers belonging to the pod.
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

<tr>

<td>

<code>sideInputs</code></br> <em> \[\]string </em>
</td>

<td>

<em>(Optional)</em>
<p>

Names of the side inputs used in this vertex.
</p>

</td>

</tr>

<tr>

<td>

<code>sideInputsContainerTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">
ContainerTemplate </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Container template for the side inputs watcher container.
</p>

</td>

</tr>

<tr>

<td>

<code>updateStrategy</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.UpdateStrategy"> UpdateStrategy
</a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

The strategy to use to replace existing pods with new ones.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.AccumulatorWindow">

AccumulatorWindow
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Window">Window</a>)
</p>

<p>

<p>

AccumulatorWindow describes a special kind of SessionWindow (similar to
Global Window) where output should always have monotonically increasing
WM but it can be manipulated through event-time by reordering the
messages. NOTE: Quite powerful, should not be abused; it can cause
stalling of pipelines and leaks.
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

<code>timeout</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<p>

Timeout is the duration of inactivity after which the state of the
accumulator is removed.
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
<a href="#numaflow.numaproj.io/v1alpha1.HTTPSource">HTTPSource</a>,
<a href="#numaflow.numaproj.io/v1alpha1.ServingSpec">ServingSpec</a>)
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

<h3 id="numaflow.numaproj.io/v1alpha1.Backoff">

Backoff
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RetryStrategy">RetryStrategy</a>)
</p>

<p>

<p>

Backoff defines parameters used to systematically configure the retry
strategy.
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

<code>interval</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Interval sets the initial retry duration, after a failure occurs.
</p>

</td>

</tr>

<tr>

<td>

<code>steps</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

Steps defines the maximum number of retry attempts
</p>

</td>

</tr>

<tr>

<td>

<code>factor</code></br> <em> float64 </em>
</td>

<td>

<em>(Optional)</em>
<p>

Interval is multiplied by factor each iteration, if factor is not zero
and the limits imposed by Steps and Cap have not been reached.
</p>

</td>

</tr>

<tr>

<td>

<code>cap</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

A limit on revised values of the interval parameter. If a multiplication
by the factor parameter would make the interval exceed the cap then the
interval is set to the cap and the steps parameter is set to zero.
</p>

</td>

</tr>

<tr>

<td>

<code>jitter</code></br> <em> float64 </em>
</td>

<td>

<em>(Optional)</em>
<p>

The sleep at each iteration is the interval plus an additional amount
chosen uniformly at random from the interval between zero and
<code>jitter\*interval</code>.
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
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink">AbstractSink</a>)
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
<a href="#numaflow.numaproj.io/v1alpha1.GetServingPipelineResourceReq">GetServingPipelineResourceReq</a>,
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

<h3 id="numaflow.numaproj.io/v1alpha1.Compression">

Compression
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBuffer">InterStepBuffer</a>)
</p>

<p>

<p>

Compression is the compression settings for the messages in the
InterStepBuffer
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

<code>type</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.CompressionType">
CompressionType </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Type is the type of compression to be used
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.CompressionType">

CompressionType (<code>string</code> alias)
</p>

</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Compression">Compression</a>)
</p>

<p>

<p>

CompressionType is a string enumeration type that enumerates all
possible compression types.
</p>

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
<a href="#numaflow.numaproj.io/v1alpha1.ServingStore">ServingStore</a>,
<a href="#numaflow.numaproj.io/v1alpha1.SideInput">SideInput</a>,
<a href="#numaflow.numaproj.io/v1alpha1.UDF">UDF</a>,
<a href="#numaflow.numaproj.io/v1alpha1.UDSink">UDSink</a>,
<a href="#numaflow.numaproj.io/v1alpha1.UDSource">UDSource</a>,
<a href="#numaflow.numaproj.io/v1alpha1.UDTransformer">UDTransformer</a>)
</p>

<p>

<p>

Container is used to define the container properties for user-defined
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

<tr>

<td>

<code>readinessProbe</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Probe"> Probe </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>livenessProbe</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Probe"> Probe </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>ports</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#containerport-v1-core">
\[\]Kubernetes core/v1.ContainerPort </a> </em>
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
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>,
<a href="#numaflow.numaproj.io/v1alpha1.ServingSpec">ServingSpec</a>,
<a href="#numaflow.numaproj.io/v1alpha1.SideInputsManagerTemplate">SideInputsManagerTemplate</a>,
<a href="#numaflow.numaproj.io/v1alpha1.VertexTemplate">VertexTemplate</a>)
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

<tr>

<td>

<code>readinessProbe</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Probe"> Probe </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>livenessProbe</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Probe"> Probe </a> </em>
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
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>,
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

Conditional forwarding, only allowed when “From” is a Source or UDF.
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

<p>

Length is the duration of the fixed window.
</p>

</td>

</tr>

<tr>

<td>

<code>streaming</code></br> <em> bool </em>
</td>

<td>

<em>(Optional)</em>
<p>

Streaming should be set to true if the reduce udf is streaming.
</p>

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

<em>(Optional)</em>
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

<em>(Optional)</em>
<p>

Value is an optional uint64 value to be written in to the payload
</p>

</td>

</tr>

<tr>

<td>

<code>jitter</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Jitter is the jitter for the message generation, used to simulate out of
order messages for example if the jitter is 10s, then the message’s
event time will be delayed by a random time between 0 and 10s which will
result in the message being out of order by 0 to 10s
</p>

</td>

</tr>

<tr>

<td>

<code>valueBlob</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
<p>

ValueBlob is an optional string which is the base64 encoding of direct
payload to send. This is useful for attaching a GeneratorSource to a
true pipeline to test load behavior with true messages without requiring
additional work to generate messages through the external source if
present, the Value and MsgSize fields will be ignored.
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

<tr>

<td>

<code>DefaultResources</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements </a> </em>
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

<tr>

<td>

<code>DefaultResources</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements </a> </em>
</td>

<td>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.GetMonoVertexDaemonDeploymentReq">

GetMonoVertexDaemonDeploymentReq
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

<tr>

<td>

<code>DefaultResources</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements </a> </em>
</td>

<td>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.GetMonoVertexPodSpecReq">

GetMonoVertexPodSpecReq
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

<tr>

<td>

<code>DefaultResources</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements </a> </em>
</td>

<td>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.GetServingPipelineResourceReq">

GetServingPipelineResourceReq
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

<code>ISBSvcConfig</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.BufferServiceConfig">
BufferServiceConfig </a> </em>
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

<tr>

<td>

<code>DefaultResources</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements </a> </em>
</td>

<td>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.GetSideInputDeploymentReq">

GetSideInputDeploymentReq
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

<tr>

<td>

<code>DefaultResources</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements </a> </em>
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

<tr>

<td>

<code>SideInputsStoreName</code></br> <em> string </em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>ServingSourceStreamName</code></br> <em> string </em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>PipelineSpec</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec"> PipelineSpec </a>
</em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>DefaultResources</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#resourcerequirements-v1-core">
Kubernetes core/v1.ResourceRequirements </a> </em>
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
<a href="#numaflow.numaproj.io/v1alpha1.GetSideInputDeploymentReq">GetSideInputDeploymentReq</a>,
<a href="#numaflow.numaproj.io/v1alpha1.GetVertexPodSpecReq">GetVertexPodSpecReq</a>,
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBufferServiceStatus">InterStepBufferServiceStatus</a>)
</p>

<p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.IdleSource">

IdleSource
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Watermark">Watermark</a>)
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

<code>threshold</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<p>

Threshold is the duration after which a source is marked as Idle due to
lack of data. Ex: If watermark found to be idle after the Threshold
duration then the watermark is progressed by <code>IncrementBy</code>.
</p>

</td>

</tr>

<tr>

<td>

<code>stepInterval</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

StepInterval is the duration between the subsequent increment of the
watermark as long the source remains Idle. The default value is 0s which
means that once we detect idle source, we will be incrementing the
watermark by <code>IncrementBy</code> for time we detect that we source
is empty (in other words, this will be a very frequent update).
</p>

</td>

</tr>

<tr>

<td>

<code>incrementBy</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<p>

IncrementBy is the duration to be added to the current watermark to
progress the watermark when source is idling.
</p>

</td>

</tr>

<tr>

<td>

<code>initSourceDelay</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

InitSourceDelay is the duration after which, if source doesn’t produce
any data, the watermark is initialized with the current wall clock time.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.InterStepBuffer">

InterStepBuffer
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec">PipelineSpec</a>,
<a href="#numaflow.numaproj.io/v1alpha1.VertexSpec">VertexSpec</a>)
</p>

<p>

<p>

InterStepBuffer configuration specifically for the pipeline.
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

<code>compression</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Compression"> Compression </a>
</em>
</td>

<td>

<em>(Optional)</em>
<p>

Compression is the compression settings for the InterStepBufferService
</p>

</td>

</tr>

</tbody>

</table>

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

<tr>

<td>

<code>observedGeneration</code></br> <em> int64 </em>
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

JetStream StatefulSet size
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

<h3 id="numaflow.numaproj.io/v1alpha1.JetStreamSource">

JetStreamSource
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

<code>stream</code></br> <em> string </em>
</td>

<td>

<p>

Stream represents the name of the stream.
</p>

</td>

</tr>

<tr>

<td>

<code>consumer</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
<p>

Consumer represents the name of the consumer of the stream If not
specified, a consumer with name
<code>numaflow-pipeline_name-vertex_name-stream_name</code> will be
created. If a consumer name is specified, a consumer with that name will
be created if it doesn’t exist on the stream.
</p>

</td>

</tr>

<tr>

<td>

<code>deliver_policy</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
<p>

The point in the stream from which to receive messages.
<a href="https://docs.nats.io/nats-concepts/jetstream/consumers#deliverpolicy">https://docs.nats.io/nats-concepts/jetstream/consumers#deliverpolicy</a>
Valid options are: “all”, “new”, “last”, “last_per_subject”,
“by_start_sequence 42”, “by_start_time 1753428483000”. The second value
to “by_start_time” is unix epoch time in milliseconds.
</p>

</td>

</tr>

<tr>

<td>

<code>filter_subjects</code></br> <em> \[\]string </em>
</td>

<td>

<em>(Optional)</em>
<p>

A set of subjects that overlap with the subjects bound to the stream to
filter delivery to subscribers.
<a href="https://docs.nats.io/nats-concepts/jetstream/consumers#filtesubjects">https://docs.nats.io/nats-concepts/jetstream/consumers#filtesubjects</a>
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
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink">AbstractSink</a>)
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

<code>setKey</code></br> <em> bool </em>
</td>

<td>

<em>(Optional)</em>
<p>

SetKey sets the Kafka key to the keys passed in the Message. When the
key is null (default), the record is sent randomly to one of the
available partitions of the topic. If a key exists, Kafka hashes the
key, and the result is used to map the message to a specific partition.
This ensures that messages with the same key end up in the same
partition.
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

<tr>

<td>

<code>kafkaVersion</code></br> <em> string </em>
</td>

<td>

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

<code>deletionGracePeriodSeconds</code></br> <em> int64 </em>
</td>

<td>

<em>(Optional)</em>
<p>

DeletionGracePeriodSeconds used to delete pipeline gracefully
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

<tr>

<td>

<code>pauseGracePeriodSeconds</code></br> <em> int64 </em>
</td>

<td>

<em>(Optional)</em>
<p>

PauseGracePeriodSeconds used to pause pipeline gracefully
</p>

</td>

</tr>

<tr>

<td>

<code>deleteGracePeriodSeconds</code></br> <em> int64 </em>
</td>

<td>

<em>(Optional)</em>
<p>

DeleteGracePeriodSeconds used to delete pipeline gracefully Deprecated:
Use DeletionGracePeriodSeconds instead
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
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink">AbstractSink</a>)
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

<h3 id="numaflow.numaproj.io/v1alpha1.MonoVertex">

MonoVertex
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
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec"> MonoVertexSpec
</a> </em>
</td>

<td>

<br/> <br/>
<table>

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

<code>source</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Source"> Source </a> </em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>sink</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Sink"> Sink </a> </em>
</td>

<td>

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
<p>

Container template for the main numa container.
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
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexLimits">
MonoVertexLimits </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Limits define the limitations such as read batch size for the mono
vertex.
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

List of customized init containers belonging to the pod. More info:
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

List of customized sidecar containers belonging to the pod.
</p>

</td>

</tr>

<tr>

<td>

<code>daemonTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.DaemonTemplate"> DaemonTemplate
</a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Template for the daemon service deployment.
</p>

</td>

</tr>

<tr>

<td>

<code>updateStrategy</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.UpdateStrategy"> UpdateStrategy
</a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

The strategy to use to replace existing pods with new ones.
</p>

</td>

</tr>

<tr>

<td>

<code>lifecycle</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexLifecycle">
MonoVertexLifecycle </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Lifecycle defines the Lifecycle properties of a MonoVertex
</p>

</td>

</tr>

</table>

</td>

</tr>

<tr>

<td>

<code>status</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexStatus">
MonoVertexStatus </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.MonoVertexLifecycle">

MonoVertexLifecycle
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>)
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

<code>desiredPhase</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexPhase">
MonoVertexPhase </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

DesiredPhase used to bring the MonoVertex from current phase to desired
phase
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.MonoVertexLimits">

MonoVertexLimits
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>)
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

Read batch size from the source.
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

ReadTimeout is the read timeout duration from the source.
</p>

</td>

</tr>

<tr>

<td>

<code>rateLimit</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimit"> RateLimit </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

RateLimit for MonoVertex defines how many messages can be read from
Source. This is computed by number of <code>read</code> calls per second
multiplied by the <code>readBatchSize</code>. This is how RateLimit is
calculated for MonoVertex and for Source vertices.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.MonoVertexPhase">

MonoVertexPhase (<code>string</code> alias)
</p>

</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexLifecycle">MonoVertexLifecycle</a>,
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexStatus">MonoVertexStatus</a>)
</p>

<p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.MonoVertexSpec">

MonoVertexSpec
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertex">MonoVertex</a>)
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

<code>replicas</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>source</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Source"> Source </a> </em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>sink</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Sink"> Sink </a> </em>
</td>

<td>

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
<p>

Container template for the main numa container.
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
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexLimits">
MonoVertexLimits </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Limits define the limitations such as read batch size for the mono
vertex.
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

List of customized init containers belonging to the pod. More info:
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

List of customized sidecar containers belonging to the pod.
</p>

</td>

</tr>

<tr>

<td>

<code>daemonTemplate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.DaemonTemplate"> DaemonTemplate
</a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Template for the daemon service deployment.
</p>

</td>

</tr>

<tr>

<td>

<code>updateStrategy</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.UpdateStrategy"> UpdateStrategy
</a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

The strategy to use to replace existing pods with new ones.
</p>

</td>

</tr>

<tr>

<td>

<code>lifecycle</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexLifecycle">
MonoVertexLifecycle </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Lifecycle defines the Lifecycle properties of a MonoVertex
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.MonoVertexStatus">

MonoVertexStatus
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertex">MonoVertex</a>)
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
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexPhase">
MonoVertexPhase </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>replicas</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

Total number of non-terminated pods targeted by this MonoVertex (their
labels match the selector).
</p>

</td>

</tr>

<tr>

<td>

<code>desiredReplicas</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

The number of desired replicas.
</p>

</td>

</tr>

<tr>

<td>

<code>selector</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>reason</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>message</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>lastUpdated</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#time-v1-meta">
Kubernetes meta/v1.Time </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>lastScaledAt</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#time-v1-meta">
Kubernetes meta/v1.Time </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Time of last scaling operation.
</p>

</td>

</tr>

<tr>

<td>

<code>observedGeneration</code></br> <em> int64 </em>
</td>

<td>

<em>(Optional)</em>
<p>

The generation observed by the MonoVertex controller.
</p>

</td>

</tr>

<tr>

<td>

<code>readyReplicas</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

The number of pods targeted by this MonoVertex with a Ready Condition.
</p>

</td>

</tr>

<tr>

<td>

<code>updatedReplicas</code></br> <em> uint32 </em>
</td>

<td>

<p>

The number of Pods created by the controller from the MonoVertex version
indicated by updateHash.
</p>

</td>

</tr>

<tr>

<td>

<code>updatedReadyReplicas</code></br> <em> uint32 </em>
</td>

<td>

<p>

The number of ready Pods created by the controller from the MonoVertex
version indicated by updateHash.
</p>

</td>

</tr>

<tr>

<td>

<code>currentHash</code></br> <em> string </em>
</td>

<td>

<p>

If not empty, indicates the current version of the MonoVertex used to
generate Pods.
</p>

</td>

</tr>

<tr>

<td>

<code>updateHash</code></br> <em> string </em>
</td>

<td>

<p>

If not empty, indicates the updated version of the MonoVertex used to
generate Pods.
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
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamSource">JetStreamSource</a>,
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

Basic auth which contains a username and a password
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

<h3 id="numaflow.numaproj.io/v1alpha1.NoStore">

NoStore
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PBQStorage">PBQStorage</a>)
</p>

<p>

<p>

NoStore means there will be no persistence storage and there will be
data loss during pod restarts. Use this option only if you do not care
about correctness (e.g., approx statistics pipeline like sampling rate,
etc.).
</p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.OnFailureRetryStrategy">

OnFailureRetryStrategy (<code>string</code> alias)
</p>

</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RetryStrategy">RetryStrategy</a>)
</p>

<p>

</p>

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

<tr>

<td>

<code>no_store</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.NoStore"> NoStore </a> </em>
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

Available access modes such as ReadWriteOncePod, ReadWriteOnce,
ReadWriteMany
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
<p>

InterStepBufferServiceName is the name of the InterStepBufferService to
be used by the pipeline
</p>

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

Templates are used to customize additional kubernetes resources required
for the Pipeline
</p>

</td>

</tr>

<tr>

<td>

<code>sideInputs</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SideInput"> \[\]SideInput </a>
</em>
</td>

<td>

<em>(Optional)</em>
<p>

SideInputs defines the Side Inputs of a pipeline.
</p>

</td>

</tr>

<tr>

<td>

<code>interStepBuffer</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBuffer">
InterStepBuffer </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

InterStepBuffer configuration specific to this pipeline.
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
by the vertex’s limit settings.
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

BufferMaxLength is used to define the max length of a buffer. Only
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

<tr>

<td>

<code>rateLimit</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimit"> RateLimit </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

RateLimit is used to define the rate limit for all the vertices in the
pipeline, it could be overridden by the vertex’s limit settings. For
source vertices, it will be set to rate divided by readBatchSize because
for source vertices, the rate limit is defined by how many times the
<code>Read</code> is called per second Reduce does not support
RateLimit.
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

<h3 id="numaflow.numaproj.io/v1alpha1.PipelineResumeStrategy">

PipelineResumeStrategy (<code>string</code> alias)
</p>

</h3>

<p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.PipelineSpec">

PipelineSpec
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.GetVertexPodSpecReq">GetVertexPodSpecReq</a>,
<a href="#numaflow.numaproj.io/v1alpha1.Pipeline">Pipeline</a>,
<a href="#numaflow.numaproj.io/v1alpha1.ServingPipelineSpec">ServingPipelineSpec</a>)
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
<p>

InterStepBufferServiceName is the name of the InterStepBufferService to
be used by the pipeline
</p>

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

Templates are used to customize additional kubernetes resources required
for the Pipeline
</p>

</td>

</tr>

<tr>

<td>

<code>sideInputs</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SideInput"> \[\]SideInput </a>
</em>
</td>

<td>

<em>(Optional)</em>
<p>

SideInputs defines the Side Inputs of a pipeline.
</p>

</td>

</tr>

<tr>

<td>

<code>interStepBuffer</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBuffer">
InterStepBuffer </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

InterStepBuffer configuration specific to this pipeline.
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

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>message</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>lastUpdated</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#time-v1-meta">
Kubernetes meta/v1.Time </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>vertexCount</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>sourceCount</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>sinkCount</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>udfCount</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>mapUDFCount</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>reduceUDFCount</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>observedGeneration</code></br> <em> int64 </em>
</td>

<td>

<em>(Optional)</em>
<p>

The generation observed by the Pipeline controller.
</p>

</td>

</tr>

<tr>

<td>

<code>drainedOnPause</code></br> <em> bool </em>
</td>

<td>

<em>(Optional)</em>
<p>

Field to indicate if a pipeline drain successfully occurred, only
meaningful when the pipeline is paused. True means it has been
successfully drained.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.Ports">

Ports
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingSpec">ServingSpec</a>)
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

<code>https</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>http</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.Probe">

Probe
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Container">Container</a>,
<a href="#numaflow.numaproj.io/v1alpha1.ContainerTemplate">ContainerTemplate</a>)
</p>

<p>

<p>

Probe is used to customize the configuration for Readiness and Liveness
probes.
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

<code>initialDelaySeconds</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

Number of seconds after the container has started before liveness probes
are initiated. More info:
<a href="https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes">https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes</a>
</p>

</td>

</tr>

<tr>

<td>

<code>timeoutSeconds</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

Number of seconds after which the probe times out. More info:
<a href="https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes">https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes</a>
</p>

</td>

</tr>

<tr>

<td>

<code>periodSeconds</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

How often (in seconds) to perform the probe.
</p>

</td>

</tr>

<tr>

<td>

<code>successThreshold</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

Minimum consecutive successes for the probe to be considered successful
after having failed. Defaults to 1. Must be 1 for liveness and startup.
Minimum value is 1.
</p>

</td>

</tr>

<tr>

<td>

<code>failureThreshold</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

Minimum consecutive failures for the probe to be considered failed after
having succeeded. Defaults to 3. Minimum value is 1.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.PulsarAuth">

PulsarAuth
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PulsarSink">PulsarSink</a>,
<a href="#numaflow.numaproj.io/v1alpha1.PulsarSource">PulsarSource</a>)
</p>

<p>

<p>

PulsarAuth defines how to authenticate with Pulsar
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

<code>token</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

JWT Token auth
</p>

</td>

</tr>

<tr>

<td>

<code>basicAuth</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PulsarBasicAuth">
PulsarBasicAuth </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Authentication using HTTP basic
<a href="https://pulsar.apache.org/docs/4.0.x/security-basic-auth/">https://pulsar.apache.org/docs/4.0.x/security-basic-auth/</a>
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.PulsarBasicAuth">

PulsarBasicAuth
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PulsarAuth">PulsarAuth</a>)
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

<code>username</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>

<td>

<em>(Optional)</em>
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
</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.PulsarSink">

PulsarSink
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink">AbstractSink</a>)
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

<code>serverAddr</code></br> <em> string </em>
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

<code>producerName</code></br> <em> string </em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>auth</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PulsarAuth"> PulsarAuth </a>
</em>
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

<h3 id="numaflow.numaproj.io/v1alpha1.PulsarSource">

PulsarSource
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

<code>serverAddr</code></br> <em> string </em>
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

<code>consumerName</code></br> <em> string </em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>subscriptionName</code></br> <em> string </em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>maxUnack</code></br> <em> uint32 </em>
</td>

<td>

<p>

Maximum number of messages that are in not yet acked state. Once this
limit is crossed, futher read requests will return empty list.
</p>

</td>

</tr>

<tr>

<td>

<code>auth</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PulsarAuth"> PulsarAuth </a>
</em>
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

<h3 id="numaflow.numaproj.io/v1alpha1.RateLimit">

RateLimit
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexLimits">MonoVertexLimits</a>,
<a href="#numaflow.numaproj.io/v1alpha1.PipelineLimits">PipelineLimits</a>,
<a href="#numaflow.numaproj.io/v1alpha1.VertexLimits">VertexLimits</a>)
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

<code>max</code></br> <em> uint64 </em>
</td>

<td>

<p>

Max is the maximum TPS that this vertex can process give a distributed
<code>Store</code> is configured. Otherwise, it will be the maximum TPS
for a single replica.
</p>

</td>

</tr>

<tr>

<td>

<code>min</code></br> <em> uint64 </em>
</td>

<td>

<p>

Minimum TPS allowed during initial bootup. This value will be
distributed across all the replicas if a distributed <code>Store</code>
is configured. Otherwise, it will be the minimum TPS for a single
replica.
</p>

</td>

</tr>

<tr>

<td>

<code>rampUpDuration</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<p>

RampUpDuration is the duration to reach the maximum TPS from the minimum
TPS. The min unit of ramp up is 1 in 1 second.
</p>

</td>

</tr>

<tr>

<td>

<code>store</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterStore">
RateLimiterStore </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Store is used to define the Distributed Store for the rate limiting. We
also support in-memory store if no store is configured. This means that
every replica will have its own rate limit and the actual TPS will be
the sum of all the replicas.
</p>

</td>

</tr>

<tr>

<td>

<code>modes</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterModes">
RateLimiterModes </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

RateLimiterModes is used to define the modes for rate limiting.
</p>

</td>

</tr>

<tr>

<td>

<code>resumedRampUp</code></br> <em> bool </em>
</td>

<td>

<em>(Optional)</em>
<p>

ResumedRampUp is used to enable the resume mode for rate limiting.
</p>

<p>

This, if true, will allow the processor to resume the ramp-up process
from the last known state of the rate limiter, i.e., if the processor
was allowed X tokens before shutting down, it will be allowed X tokens
again after the processor restarts.
</p>

<p>

The resumed ramp-up process will be allowed until TTL time after the
processor first deregisters with the rate limiter.
</p>

</td>

</tr>

<tr>

<td>

<code>ttl</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

TTL is used to define the duration after which a pod is considered stale
and removed from the pool of pods if it doesn’t sync with the rate
limiter.
</p>

<p>

Furthermore, if the ResumedRampUp is true, then TTL also defines the
amount of time within which, if a pod re-registers / registers with the
same name, with the rate limiter, it will be assigned the same rate
limit as the previous pod with that name.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.RateLimiterGoBackN">

RateLimiterGoBackN
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterModes">RateLimiterModes</a>)
</p>

<p>

<p>

RateLimiterGoBackN is for the GoBackN mode. Releases additional tokens
only when previously released tokens have been utilized above the
configured threshold otherwise triggers a ramp-down. Ramp-down is also
triggered when the request is made after quite a while.
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

<code>coolDownPeriod</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

CoolDownPeriod is the duration after which the rate limiter will start
ramping down if the request is made after the cool-down period.
</p>

</td>

</tr>

<tr>

<td>

<code>rampDownPercentage</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

RampDownStrength is the strength of the ramp-down. It is a value between
0 and 1. 0 means no ramp-down and 1 means token pool is ramped down at
the rate of slope=(max - min)/duration.
</p>

</td>

</tr>

<tr>

<td>

<code>thresholdPercentage</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

ThresholdPercentage specifies the minimum percentage of capacity,
availed by the rate limiter, that should be consumed at any instance to
allow the rate limiter to unlock additional capacity. For example, given
the following configuration: - max = 100 - min = 10 - rampUpDuration =
10s i.e.–\> slope = 10 messages/second - thresholdPercentage = 50 at t =
0, the rate limiter will release 10 messages and at least 5 of those
should be consumed to unlock additional capacity of 10 messages at t = 1
to make the total capacity of 20.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.RateLimiterInMemoryStore">

RateLimiterInMemoryStore
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterStore">RateLimiterStore</a>)
</p>

<p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.RateLimiterModes">

RateLimiterModes
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimit">RateLimit</a>)
</p>

<p>

<p>

RateLimiterModes defines the modes for rate limiting.
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

<code>scheduled</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterScheduled">
RateLimiterScheduled </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Irrespective of the traffic, the rate limiter releases max possible
tokens based on ramp-up duration.
</p>

</td>

</tr>

<tr>

<td>

<code>relaxed</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterRelaxed">
RateLimiterRelaxed </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

If there is some traffic, then release the max possible tokens.
</p>

</td>

</tr>

<tr>

<td>

<code>onlyIfUsed</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterOnlyIfUsed">
RateLimiterOnlyIfUsed </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Releases additional tokens only when previously released tokens have
been utilized above the configured threshold
</p>

</td>

</tr>

<tr>

<td>

<code>goBackN</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterGoBackN">
RateLimiterGoBackN </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Releases additional tokens only when previously released tokens have
been utilized above the configured threshold otherwise triggers a
ramp-down. Ramp-down is also triggered when the request is made after
quite a while.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.RateLimiterOnlyIfUsed">

RateLimiterOnlyIfUsed
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterModes">RateLimiterModes</a>)
</p>

<p>

<p>

RateLimiterOnlyIfUsed is for the OnlyIfUsed mode. Releases additional
tokens only when previously released tokens have been utilized above the
configured threshold
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

<code>thresholdPercentage</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

ThresholdPercentage specifies the minimum percentage of capacity,
availed by the rate limiter, that should be consumed at any instance to
allow the rate limiter to unlock additional capacity.
</p>

<p>

Defaults to 50%
</p>

<p>

For example, given the following configuration: - max = 100 - min = 10 -
rampUpDuration = 10s i.e.–\> slope = 10 messages/second -
thresholdPercentage = 50 at t = 0, the rate limiter will release 10
messages and at least 5 of those should be consumed to unlock additional
capacity of 10 messages at t = 1 to make the total capacity of 20.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.RateLimiterRedisStore">

RateLimiterRedisStore
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterStore">RateLimiterStore</a>)
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

<code>mode</code></br> <em> string </em>
</td>

<td>

<p>

Choose how to connect to Redis. - Single: use a single URL (redis://… or
rediss://…) - Sentinel: discover the node via Redis Sentinel
</p>

</td>

</tr>

<tr>

<td>

<code>url</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
<p>

SINGLE MODE: Full connection URL,
e.g. redis://host:<sup>6379</sup>⁄<sub>0</sub> or rediss://host:port/0
Mutually exclusive with .sentinel
</p>

</td>

</tr>

<tr>

<td>

<code>sentinel</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisSentinelConfig">
RedisSentinelConfig </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

SENTINEL MODE: Settings to reach Sentinel and the selected Redis node
Mutually exclusive with .url
</p>

</td>

</tr>

<tr>

<td>

<code>db</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

COMMON: Optional DB index (default 0)
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.RateLimiterRelaxed">

RateLimiterRelaxed
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterModes">RateLimiterModes</a>)
</p>

<p>

<p>

RateLimiterRelaxed is for the relaxed mode. It will release the max
possible tokens if there is some traffic.
</p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.RateLimiterScheduled">

RateLimiterScheduled
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterModes">RateLimiterModes</a>)
</p>

<p>

<p>

RateLimiterScheduled is for the scheduled mode. It will release the max
possible tokens based on ramp-up duration irrespective of traffic
encountered.
</p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.RateLimiterStore">

RateLimiterStore
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimit">RateLimit</a>)
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

<code>redisStore</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterRedisStore">
RateLimiterRedisStore </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

RedisStore is used to define the redis store for the rate limit.
</p>

</td>

</tr>

<tr>

<td>

<code>inMemoryStore</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterInMemoryStore">
RateLimiterInMemoryStore </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

InMemoryStore is used to define the in-memory store for the rate limit.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.RedisAuth">

RedisAuth
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisSentinelConfig">RedisSentinelConfig</a>)
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

<code>username</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

For Redis 6+ ACLs. If Username omitted, password-only is also supported.
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
</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.RedisSentinelConfig">

RedisSentinelConfig
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimiterRedisStore">RateLimiterRedisStore</a>)
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

<code>masterName</code></br> <em> string </em>
</td>

<td>

<p>

Required Sentinel “service name” (aka master name) from sentinel.conf
</p>

</td>

</tr>

<tr>

<td>

<code>endpoints</code></br> <em> \[\]string </em>
</td>

<td>

<p>

At least one Sentinel endpoint; 2–3 recommended. Use host:port pairs.
Example: \[“sentinel-0.redis.svc:26379”, “sentinel-1.redis.svc:26379”\]
</p>

</td>

</tr>

<tr>

<td>

<code>sentinelAuth</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisAuth"> RedisAuth </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Auth to talk to the Sentinel daemons (control-plane). Optional.
</p>

</td>

</tr>

<tr>

<td>

<code>redisAuth</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RedisAuth"> RedisAuth </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Auth to talk to the Redis data nodes (data-plane). Optional.
</p>

</td>

</tr>

<tr>

<td>

<code>sentinelTLS</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.TLS"> TLS </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

TLS for Sentinel connections (if your Sentinels expose TLS).
</p>

</td>

</tr>

<tr>

<td>

<code>redisTLS</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.TLS"> TLS </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

TLS for Redis data nodes (redis). Often enabled even if Sentinel is
plaintext.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.RetryStrategy">

RetryStrategy
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Sink">Sink</a>)
</p>

<p>

<p>

The RetryStrategy struct defines the configuration for handling
operation retries in case of failures. It incorporates an Exponential
BackOff strategy to control retry timing and specifies the actions to
take upon failure.
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

<code>backoff</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Backoff"> Backoff </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

BackOff specifies the parameters for the exponential backoff strategy,
controlling how delays between retries should increase.
</p>

</td>

</tr>

<tr>

<td>

<code>onFailure</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.OnFailureRetryStrategy">
OnFailureRetryStrategy </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

OnFailure specifies the action to take when the specified retry strategy
fails. The possible values are: 1. “retry”: start another round of
retrying the operation, 2. “fallback”: re-route the operation to a
fallback sink and 3. “drop”: drop the operation and perform no further
action. The default action is to retry.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.RollingUpdateStrategy">

RollingUpdateStrategy
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.UpdateStrategy">UpdateStrategy</a>)
</p>

<p>

<p>

RollingUpdateStrategy is used to communicate parameter for
RollingUpdateStrategyType.
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

<code>maxUnavailable</code></br> <em>
k8s.io/apimachinery/pkg/util/intstr.IntOrString </em>
</td>

<td>

<em>(Optional)</em>
<p>

The maximum number of pods that can be unavailable during the update.
Value can be an absolute number (ex: 5) or a percentage of desired pods
(ex: 10%). Absolute number is calculated from percentage by rounding
down. Defaults to 25%. Example: when this is set to 30%, the old pods
can be scaled down to 70% of desired pods immediately when the rolling
update starts. Once new pods are ready, old pods can be scaled down
further, followed by scaling up the new pods, ensuring that the total
number of pods available at all times during the update is at least 70%
of desired pods.
</p>

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

<tr>

<td>

<code>scramsha256</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SASLPlain"> SASLPlain </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

SASLSCRAMSHA256 contains the sasl plain config
</p>

</td>

</tr>

<tr>

<td>

<code>scramsha512</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SASLPlain"> SASLPlain </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

SASLSCRAMSHA512 contains the sasl plain config
</p>

</td>

</tr>

<tr>

<td>

<code>oauth</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SASLOAuth"> SASLOAuth </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

OAuth contains the oauth config
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.SASLOAuth">

SASLOAuth
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

<code>clientID</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>

<td>

<p>

ClientID refers to the secret that contains the client id
</p>

</td>

</tr>

<tr>

<td>

<code>clientSecret</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#secretkeyselector-v1-core">
Kubernetes core/v1.SecretKeySelector </a> </em>
</td>

<td>

<p>

ClientSecret refers to the secret that contains the client secret
</p>

</td>

</tr>

<tr>

<td>

<code>tokenEndpoint</code></br> <em> string </em>
</td>

<td>

<p>

TokenEndpoint refers to the token endpoint
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
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>)
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

<code>zeroReplicaSleepSeconds</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

After scaling down the source vertex to 0, sleep how many seconds before
scaling the source vertex back up to peek.
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

DeprecatedReplicasPerScale defines the number of maximum replicas that
can be changed in a single scale up or down operation. The is use to
prevent from too aggressive scaling operations Deprecated: Use
ReplicasPerScaleUp and ReplicasPerScaleDown instead
</p>

</td>

</tr>

<tr>

<td>

<code>scaleUpCooldownSeconds</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

ScaleUpCooldownSeconds defines the cooldown seconds after a scaling
operation, before a follow-up scaling up. It defaults to the
CooldownSeconds if not set.
</p>

</td>

</tr>

<tr>

<td>

<code>scaleDownCooldownSeconds</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

ScaleDownCooldownSeconds defines the cooldown seconds after a scaling
operation, before a follow-up scaling down. It defaults to the
CooldownSeconds if not set.
</p>

</td>

</tr>

<tr>

<td>

<code>replicasPerScaleUp</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

ReplicasPerScaleUp defines the number of maximum replicas that can be
changed in a single scaled up operation. The is use to prevent from too
aggressive scaling up operations
</p>

</td>

</tr>

<tr>

<td>

<code>replicasPerScaleDown</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

ReplicasPerScaleDown defines the number of maximum replicas that can be
changed in a single scaled down operation. The is use to prevent from
too aggressive scaling down operations
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.ServeSink">

ServeSink
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink">AbstractSink</a>)
</p>

<p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.ServingPipeline">

ServingPipeline
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
<a href="#numaflow.numaproj.io/v1alpha1.ServingPipelineSpec">
ServingPipelineSpec </a> </em>
</td>

<td>

<br/> <br/>
<table>

<tr>

<td>

<code>serving</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingSpec"> ServingSpec </a>
</em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>pipeline</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec"> PipelineSpec </a>
</em>
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
<a href="#numaflow.numaproj.io/v1alpha1.ServingPipelineStatus">
ServingPipelineStatus </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.ServingPipelinePhase">

ServingPipelinePhase (<code>string</code> alias)
</p>

</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingPipelineStatus">ServingPipelineStatus</a>)
</p>

<p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.ServingPipelineSpec">

ServingPipelineSpec
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingPipeline">ServingPipeline</a>)
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

<code>serving</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingSpec"> ServingSpec </a>
</em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>pipeline</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec"> PipelineSpec </a>
</em>
</td>

<td>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.ServingPipelineStatus">

ServingPipelineStatus
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingPipeline">ServingPipeline</a>)
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
<a href="#numaflow.numaproj.io/v1alpha1.ServingPipelinePhase">
ServingPipelinePhase </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>message</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>lastUpdated</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#time-v1-meta">
Kubernetes meta/v1.Time </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>observedGeneration</code></br> <em> int64 </em>
</td>

<td>

<em>(Optional)</em>
<p>

The generation observed by the ServingPipeline controller.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.ServingSource">

ServingSource
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Source">Source</a>)
</p>

<p>

<p>

ServingSource is the source vertex for ServingPipeline and should be
used only with ServingPipeline.
</p>

</p>

<h3 id="numaflow.numaproj.io/v1alpha1.ServingSpec">

ServingSpec
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingPipelineSpec">ServingPipelineSpec</a>)
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

<tr>

<td>

<code>ports</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Ports"> Ports </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Ports to listen on, default we will use 8443 for HTTPS. To start http
server the http port should be explicitly set.
</p>

</td>

</tr>

<tr>

<td>

<code>msgIDHeaderKey</code></br> <em> string </em>
</td>

<td>

<p>

The header key from which the message id will be extracted
</p>

</td>

</tr>

<tr>

<td>

<code>requestTimeoutSeconds</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

Request timeout in seconds. Default value is 120 seconds.
</p>

</td>

</tr>

<tr>

<td>

<code>store</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingStore"> ServingStore </a>
</em>
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
<p>

Container template for the serving container.
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

Initial replicas of the serving server deployment.
</p>

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

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.ServingStore">

ServingStore
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingSpec">ServingSpec</a>)
</p>

<p>

<p>

ServingStore defines information of a Serving Store used in a pipeline
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

<code>container</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Container"> Container </a> </em>
</td>

<td>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.SessionWindow">

SessionWindow
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Window">Window</a>)
</p>

<p>

<p>

SessionWindow describes a session window
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

<code>timeout</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<p>

Timeout is the duration of inactivity after which a session window
closes.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.SideInput">

SideInput
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.PipelineSpec">PipelineSpec</a>)
</p>

<p>

<p>

SideInput defines information of a Side Input
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

<code>name</code></br> <em> string </em>
</td>

<td>

</td>

</tr>

<tr>

<td>

<code>container</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.Container"> Container </a> </em>
</td>

<td>

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

<code>trigger</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SideInputTrigger">
SideInputTrigger </a> </em>
</td>

<td>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.SideInputTrigger">

SideInputTrigger
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.SideInput">SideInput</a>)
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

<code>schedule</code></br> <em> string </em>
</td>

<td>

<p>

The schedule to trigger the retrievement of the side input data. It
supports cron format, for example, “0 30 \* \* \* \*”. Or interval based
format, such as “@hourly&rdquo;, “@every 1h30m”, etc.
</p>

</td>

</tr>

<tr>

<td>

<code>timezone</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.SideInputsManagerTemplate">

SideInputsManagerTemplate
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
<p>

Template for the side inputs manager numa container
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
<p>

Template for the side inputs manager init container
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
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>)
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

<code>AbstractSink</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink"> AbstractSink </a>
</em>
</td>

<td>

<p>

(Members of <code>AbstractSink</code> are embedded into this type.)
</p>

</td>

</tr>

<tr>

<td>

<code>fallback</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink"> AbstractSink </a>
</em>
</td>

<td>

<em>(Optional)</em>
<p>

Fallback sink can be imagined as DLQ for primary Sink. The writes to
Fallback sink will only be initiated if the ud-sink response field sets
it.
</p>

</td>

</tr>

<tr>

<td>

<code>onSuccess</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink"> AbstractSink </a>
</em>
</td>

<td>

<em>(Optional)</em>
<p>

OnSuccess sink allows triggering a secondary sink operation only after
the primary sink completes successfully The writes to OnSuccess sink
will only be initiated if the ud-sink response field sets it. A new
Message crafted in the Primary sink can be written on the OnSuccess
sink.
</p>

</td>

</tr>

<tr>

<td>

<code>retryStrategy</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RetryStrategy"> RetryStrategy
</a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

RetryStrategy struct encapsulates the settings for retrying operations
in the event of failures.
</p>

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

<p>

Length is the duration of the sliding window.
</p>

</td>

</tr>

<tr>

<td>

<code>slide</code></br> <em>
<a href="https://pkg.go.dev/k8s.io/apimachinery/pkg/apis/meta/v1#Duration">
Kubernetes meta/v1.Duration </a> </em>
</td>

<td>

<p>

Slide is the slide parameter that controls the frequency at which the
sliding window is created.
</p>

</td>

</tr>

<tr>

<td>

<code>streaming</code></br> <em> bool </em>
</td>

<td>

<em>(Optional)</em>
<p>

Streaming should be set to true if the reduce udf is streaming.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.Source">

Source
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>)
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

<code>transformer</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.UDTransformer"> UDTransformer
</a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>udsource</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.UDSource"> UDSource </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>jetstream</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamSource">
JetStreamSource </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>serving</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.ServingSource"> ServingSource
</a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>pulsar</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.PulsarSource"> PulsarSource </a>
</em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>sqs</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SqsSource"> SqsSource </a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.SqsSink">

SqsSink
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink">AbstractSink</a>)
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

<code>awsRegion</code></br> <em> string </em>
</td>

<td>

<p>

AWSRegion is the AWS Region where the SQS queue is located
</p>

</td>

</tr>

<tr>

<td>

<code>queueName</code></br> <em> string </em>
</td>

<td>

<p>

QueueName is the name of the SQS queue
</p>

</td>

</tr>

<tr>

<td>

<code>queueOwnerAWSAccountID</code></br> <em> string </em>
</td>

<td>

<p>

QueueOwnerAWSAccountID is the queue owner aws account id
</p>

</td>

</tr>

<tr>

<td>

<code>assumeRole</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AWSAssumeRole"> AWSAssumeRole
</a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

AssumeRole contains the configuration for AWS STS assume role. When
specified, the SQS client will assume the specified role for
authentication.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.SqsSource">

SqsSource
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.Source">Source</a>)
</p>

<p>

<p>

SqsSource represents the configuration of an AWS SQS source
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

<code>awsRegion</code></br> <em> string </em>
</td>

<td>

<p>

AWSRegion is the AWS Region where the SQS queue is located
</p>

</td>

</tr>

<tr>

<td>

<code>queueName</code></br> <em> string </em>
</td>

<td>

<p>

QueueName is the name of the SQS queue
</p>

</td>

</tr>

<tr>

<td>

<code>queueOwnerAWSAccountID</code></br> <em> string </em>
</td>

<td>

<p>

QueueOwnerAWSAccountID is the queue owner aws account id
</p>

</td>

</tr>

<tr>

<td>

<code>visibilityTimeout</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

VisibilityTimeout is the duration (in seconds) that the received
messages are hidden from subsequent retrieve requests after being
retrieved by a ReceiveMessage request. Valid values: 0-43200 (12 hours)
</p>

</td>

</tr>

<tr>

<td>

<code>maxNumberOfMessages</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

MaxNumberOfMessages is the maximum number of messages to return in a
single poll. Valid values: 1-10 Defaults to 1
</p>

</td>

</tr>

<tr>

<td>

<code>waitTimeSeconds</code></br> <em> int32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

WaitTimeSeconds is the duration (in seconds) for which the call waits
for a message to arrive in the queue before returning. If a message is
available, the call returns sooner than WaitTimeSeconds. Valid values:
0-20 Defaults to 0 (short polling)
</p>

</td>

</tr>

<tr>

<td>

<code>endpointUrl</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
<p>

EndpointURL is the custom endpoint URL for the AWS SQS API. This is
useful for testing with localstack or when using VPC endpoints.
</p>

</td>

</tr>

<tr>

<td>

<code>attributeNames</code></br> <em> \[\]string </em>
</td>

<td>

<em>(Optional)</em>
<p>

AttributeNames is a list of attributes that need to be returned along
with each message. Valid values: All \| Policy \| VisibilityTimeout \|
MaximumMessageSize \| MessageRetentionPeriod \|
ApproximateNumberOfMessages \| ApproximateNumberOfMessagesNotVisible \|
CreatedTimestamp \| LastModifiedTimestamp \| QueueArn \|
ApproximateNumberOfMessagesDelayed \| DelaySeconds \|
ReceiveMessageWaitTimeSeconds \| RedrivePolicy \| FifoQueue \|
ContentBasedDeduplication \| KmsMasterKeyId \|
KmsDataKeyReusePeriodSeconds \| DeduplicationScope \|
FifoThroughputLimit \| RedriveAllowPolicy \| SqsManagedSseEnabled
</p>

</td>

</tr>

<tr>

<td>

<code>messageAttributeNames</code></br> <em> \[\]string </em>
</td>

<td>

<em>(Optional)</em>
<p>

MessageAttributeNames is a list of message attributes that need to be
returned along with each message.
</p>

</td>

</tr>

<tr>

<td>

<code>assumeRole</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AWSAssumeRole"> AWSAssumeRole
</a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

AssumeRole contains the configuration for AWS STS assume role. When
specified, the SQS client will assume the specified role for
authentication.
</p>

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
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexStatus">MonoVertexStatus</a>,
<a href="#numaflow.numaproj.io/v1alpha1.PipelineStatus">PipelineStatus</a>,
<a href="#numaflow.numaproj.io/v1alpha1.ServingPipelineStatus">ServingPipelineStatus</a>,
<a href="#numaflow.numaproj.io/v1alpha1.VertexStatus">VertexStatus</a>)
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
<a href="#numaflow.numaproj.io/v1alpha1.JetStreamSource">JetStreamSource</a>,
<a href="#numaflow.numaproj.io/v1alpha1.KafkaSink">KafkaSink</a>,
<a href="#numaflow.numaproj.io/v1alpha1.KafkaSource">KafkaSource</a>,
<a href="#numaflow.numaproj.io/v1alpha1.NatsSource">NatsSource</a>,
<a href="#numaflow.numaproj.io/v1alpha1.RedisSentinelConfig">RedisSentinelConfig</a>)
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

<code>certSecret</code></br> <em>
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

<code>keySecret</code></br> <em>
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

DaemonTemplate is used to customize the Daemon Deployment.
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

JobTemplate is used to customize Jobs.
</p>

</td>

</tr>

<tr>

<td>

<code>sideInputsManager</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SideInputsManagerTemplate">
SideInputsManagerTemplate </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

SideInputsManagerTemplate is used to customize the Side Inputs Manager.
</p>

</td>

</tr>

<tr>

<td>

<code>vertex</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexTemplate"> VertexTemplate
</a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

VertexTemplate is used to customize the vertices of the pipeline.
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
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>)
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
<a href="#numaflow.numaproj.io/v1alpha1.AbstractSink">AbstractSink</a>)
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

<h3 id="numaflow.numaproj.io/v1alpha1.UDSource">

UDSource
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

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.UpdateStrategy">

UpdateStrategy
</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.AbstractVertex">AbstractVertex</a>,
<a href="#numaflow.numaproj.io/v1alpha1.MonoVertexSpec">MonoVertexSpec</a>)
</p>

<p>

<p>

UpdateStrategy indicates the strategy that the controller will use to
perform updates for Vertex or MonoVertex.
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

<code>type</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.UpdateStrategyType">
UpdateStrategyType </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Type indicates the type of the StatefulSetUpdateStrategy. Default is
RollingUpdate.
</p>

</td>

</tr>

<tr>

<td>

<code>rollingUpdate</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RollingUpdateStrategy">
RollingUpdateStrategy </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

RollingUpdate is used to communicate parameters when Type is
RollingUpdateStrategy.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.UpdateStrategyType">

UpdateStrategyType (<code>string</code> alias)
</p>

</h3>

<p>

(<em>Appears on:</em>
<a href="#numaflow.numaproj.io/v1alpha1.UpdateStrategy">UpdateStrategy</a>)
</p>

<p>

<p>

UpdateStrategyType is a string enumeration type that enumerates all
possible update strategies.
</p>

</p>

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

<tr>

<td>

<code>lifecycle</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexLifecycle">
VertexLifecycle </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Lifecycle defines the Lifecycle properties of a vertex
</p>

</td>

</tr>

<tr>

<td>

<code>interStepBuffer</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBuffer">
InterStepBuffer </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

InterStepBuffer configuration specific to this pipeline.
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

<h3 id="numaflow.numaproj.io/v1alpha1.VertexLifecycle">

VertexLifecycle
</h3>

<p>

(<em>Appears on:</em>
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

<code>desiredPhase</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexPhase"> VertexPhase </a>
</em>
</td>

<td>

<em>(Optional)</em>
<p>

DesiredPhase used to bring the vertex from current phase to desired
phase
</p>

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

<tr>

<td>

<code>rateLimit</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.RateLimit"> RateLimit </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

RateLimit is used to define the rate limit for the vertex, it overrides
the settings from pipeline limits. For Source vertices, the rate limit
is defined by how many times the <code>Read</code> is called per second
multiplied by the <code>readBatchSize</code>. Pipeline level rate limit
is not applied to Source vertices.
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
<a href="#numaflow.numaproj.io/v1alpha1.VertexLifecycle">VertexLifecycle</a>,
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

<tr>

<td>

<code>lifecycle</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.VertexLifecycle">
VertexLifecycle </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Lifecycle defines the Lifecycle properties of a vertex
</p>

</td>

</tr>

<tr>

<td>

<code>interStepBuffer</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.InterStepBuffer">
InterStepBuffer </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

InterStepBuffer configuration specific to this pipeline.
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
<a href="#numaflow.numaproj.io/v1alpha1.VertexPhase"> VertexPhase </a>
</em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>replicas</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

Total number of non-terminated pods targeted by this Vertex (their
labels match the selector).
</p>

</td>

</tr>

<tr>

<td>

<code>desiredReplicas</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

The number of desired replicas.
</p>

</td>

</tr>

<tr>

<td>

<code>selector</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>reason</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>message</code></br> <em> string </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>lastScaledAt</code></br> <em>
<a href="https://v1-18.docs.kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#time-v1-meta">
Kubernetes meta/v1.Time </a> </em>
</td>

<td>

<em>(Optional)</em>
<p>

Time of last scaling operation.
</p>

</td>

</tr>

<tr>

<td>

<code>observedGeneration</code></br> <em> int64 </em>
</td>

<td>

<em>(Optional)</em>
<p>

The generation observed by the Vertex controller.
</p>

</td>

</tr>

<tr>

<td>

<code>readyReplicas</code></br> <em> uint32 </em>
</td>

<td>

<em>(Optional)</em>
<p>

The number of pods targeted by this Vertex with a Ready Condition.
</p>

</td>

</tr>

<tr>

<td>

<code>updatedReplicas</code></br> <em> uint32 </em>
</td>

<td>

<p>

The number of Pods created by the controller from the Vertex version
indicated by updateHash.
</p>

</td>

</tr>

<tr>

<td>

<code>updatedReadyReplicas</code></br> <em> uint32 </em>
</td>

<td>

<p>

The number of ready Pods created by the controller from the Vertex
version indicated by updateHash.
</p>

</td>

</tr>

<tr>

<td>

<code>currentHash</code></br> <em> string </em>
</td>

<td>

<p>

If not empty, indicates the current version of the Vertex used to
generate Pods.
</p>

</td>

</tr>

<tr>

<td>

<code>updateHash</code></br> <em> string </em>
</td>

<td>

<p>

If not empty, indicates the updated version of the Vertex used to
generate Pods.
</p>

</td>

</tr>

</tbody>

</table>

<h3 id="numaflow.numaproj.io/v1alpha1.VertexTemplate">

VertexTemplate
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
<p>

Template for the vertex numa container
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
<p>

Template for the vertex init container
</p>

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

<tr>

<td>

<code>idleSource</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.IdleSource"> IdleSource </a>
</em>
</td>

<td>

<em>(Optional)</em>
<p>

IdleSource defines the idle watermark properties, it could be configured
in case source is idling.
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

<tr>

<td>

<code>session</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.SessionWindow"> SessionWindow
</a> </em>
</td>

<td>

<em>(Optional)</em>
</td>

</tr>

<tr>

<td>

<code>accumulator</code></br> <em>
<a href="#numaflow.numaproj.io/v1alpha1.AccumulatorWindow">
AccumulatorWindow </a> </em>
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
