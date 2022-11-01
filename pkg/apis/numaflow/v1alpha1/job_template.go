package v1alpha1

type JobTemplate struct {
	// +optional
	AbstractPodTemplate `json:",inline" protobuf:"bytes,1,opt,name=abstractPodTemplate"`
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,2,opt,name=containerTemplate"`
	// ActiveDeadlineSeconds specifies the duration in seconds relative to the startTime that the job
	// may be continuously active before the system tries to terminate it; value
	// must be positive integer.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/job/#job-termination-and-cleanup
	// Numaflow defaults to 30
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" protobuf:"varint,3,opt,name=activeDeadlineSeconds"`
	// Specifies the number of retries before marking this job failed.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-backoff-failure-policy
	// Numaflow defaults to 20
	// +optional
	BackoffLimit *int32 `json:"backoffLimit,omitempty" protobuf:"varint,4,opt,name=backoffLimit"`
}
