package v1alpha1

import corev1 "k8s.io/api/core/v1"

type JobTemplate struct {
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,1,opt,name=containerTemplate"`
	// Metadata sets the pods's metadata, i.e. annotations and labels
	// +optional
	Metadata *Metadata `json:"metadata,omitempty" protobuf:"bytes,2,opt,name=metadata"`
	// NodeSelector is a selector which must be true for the pod to fit on a node.
	// Selector which must match a node's labels for the pod to be scheduled on that node.
	// More info: https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty" protobuf:"bytes,3,rep,name=nodeSelector"`
	// If specified, the pod's tolerations.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty" protobuf:"bytes,4,rep,name=tolerations"`
	// SecurityContext holds pod-level security attributes and common container settings.
	// Optional: Defaults to empty.  See type description for default values of each field.
	// +optional
	SecurityContext *corev1.PodSecurityContext `json:"securityContext,omitempty" protobuf:"bytes,5,opt,name=securityContext"`
	// ImagePullSecrets is an optional list of references to secrets in the same namespace to use for pulling any of the images used by this PodSpec.
	// If specified, these secrets will be passed to individual puller implementations for them to use. For example,
	// in the case of docker, only DockerConfig type secrets are honored.
	// More info: https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,6,rep,name=imagePullSecrets"`
	// If specified, indicates the Redis pod's priority. "system-node-critical"
	// and "system-cluster-critical" are two special keywords which indicate the
	// highest priorities with the former being the highest priority. Any other
	// name must be defined by creating a PriorityClass object with that name.
	// If not specified, the pod priority will be default or zero if there is no
	// default.
	// More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,7,opt,name=priorityClassName"`
	// The priority value. Various system components use this field to find the
	// priority of the Redis pod. When Priority Admission Controller is enabled,
	// it prevents users from setting this field. The admission controller populates
	// this field from PriorityClassName.
	// The higher the value, the higher the priority.
	// More info: https://kubernetes.io/docs/concepts/configuration/pod-priority-preemption/
	// +optional
	Priority *int32 `json:"priority,omitempty" protobuf:"bytes,8,opt,name=priority"`
	// The pod's scheduling constraints
	// More info: https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty" protobuf:"bytes,9,opt,name=affinity"`
	// ServiceAccountName to apply to the Deployment
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty" protobuf:"bytes,10,opt,name=serviceAccountName"`
	// ActiveDeadlineSeconds specifies the duration in seconds relative to the startTime that the job
	// may be continuously active before the system tries to terminate it; value
	// must be positive integer.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/job/#job-termination-and-cleanup
	// Numaflow defaults to 30
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" protobuf:"varint,11,opt,name=activeDeadlineSeconds"`
	// Specifies the number of retries before marking this job failed.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-backoff-failure-policy
	// Numaflow defaults to 20
	// +optional
	BackoffLimit *int32 `json:"backoffLimit,omitempty" protobuf:"varint,12,opt,name=backoffLimit"`
}
