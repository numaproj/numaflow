package v1alpha1

type KafkaSink struct {
	Brokers []string `json:"brokers,omitempty" protobuf:"bytes,1,rep,name=brokers"`
	Topic   string   `json:"topic" protobuf:"bytes,2,opt,name=topic"`
	// TLS user to configure TLS connection for kafka broker
	// TLS.enable=true default for TLS.
	// +optional
	TLS *TLS `json:"tls" protobuf:"bytes,3,opt,name=tls"`
	// +optional
	Config string `json:"config,omitempty" protobuf:"bytes,4,opt,name=config"`
	// concurrency used to concurrently send message to kafka producer.
	// +kubebuilder:default=100
	// +optional
	Concurrency uint32 `json:"concurrency,omitempty" protobuf:"varint,5,opt,name=concurrency"`
}
