package v1alpha1

// ServingSource is the HTTP endpoint for Numaflow.
type ServingSource struct {
	// Container template for the main numa container.
	// +optional
	ContainerTemplate *ContainerTemplate `json:"containerTemplate,omitempty" protobuf:"bytes,5,opt,name=containerTemplate"`
}
