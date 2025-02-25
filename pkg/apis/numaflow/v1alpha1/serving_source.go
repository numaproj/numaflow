package v1alpha1

// ServingSource is the HTTP endpoint for Numaflow.
type ServingSource struct {
	// +optional
	Auth *Authorization `json:"auth" protobuf:"bytes,1,opt,name=auth"`
	// Whether to create a ClusterIP Service
	// +optional
	Service bool `json:"service" protobuf:"bytes,2,opt,name=service"`
	// The header key from which the message id will be extracted
	MsgIDHeaderKey *string `json:"msgIDHeaderKey" protobuf:"bytes,3,opt,name=msgIDHeaderKey"`
}
