package v1alpha1

// Serving is the HTTP endpoint for Numaflow.
type Serving struct {
	// +optional
	Auth *Authorization `json:"auth" protobuf:"bytes,1,opt,name=auth"`
	// Whether to create a ClusterIP Service
	// +optional
	Service bool `json:"service" protobuf:"bytes,2,opt,name=service"`
	// The header key from which the message id will be extracted
	// +optional
	MsgIDHeaderKey *string `json:"msgIDHeaderKey" protobuf:"bytes,3,opt,name=msgIDHeaderKey"`
	// Persistent store for the callbacks for serving and tracking
	// +optional
	Store *Store `json:"store" protobuf:"bytes,4,opt,name=store"`
}

// Store to track and store data and metadata for tracking and serving.
type Store struct {
	// URL of the persistent store to write the callbacks
	URL *string `json:"url" protobuf:"bytes,1,opt,name=url"`
	// TODO auth
}
