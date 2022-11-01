package v1alpha1

type JobTemplate struct {
	// +optional
	AbstractPodTemplate `json:",inline" protobuf:"bytes,1,opt,name=abstractPodTemplate"`
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,2,opt,name=containerTemplate"`
	// ttlSecondsAfterFinished limits the lifetime of a Job that has finished
	// execution (either Complete or Failed). If this field is set,
	// ttlSecondsAfterFinished after the Job finishes, it is eligible to be
	// automatically deleted. When the Job is being deleted, its lifecycle
	// guarantees (e.g. finalizers) will be honored. If this field is unset,
	// the Job won't be automatically deleted. If this field is set to zero,
	// the Job becomes eligible to be deleted immediately after it finishes.
	// Numaflow defaults to 30
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty" protobuf:"varint,3,opt,name=ttlSecondsAfterFinished"`
	// Specifies the number of retries before marking this job failed.
	// More info: https://kubernetes.io/docs/concepts/workloads/controllers/job/#pod-backoff-failure-policy
	// Numaflow defaults to 20
	// +optional
	BackoffLimit *int32 `json:"backoffLimit,omitempty" protobuf:"varint,4,opt,name=backoffLimit"`
}
