package v1alpha1

type KafkaSource struct {
	Brokers           []string `json:"brokers,omitempty" protobuf:"bytes,1,rep,name=brokers"`
	Topic             string   `json:"topic" protobuf:"bytes,2,opt,name=topic"`
	ConsumerGroupName string   `json:"consumerGroup,omitempty" protobuf:"bytes,3,opt,name=consumerGroup"`
	// TLS user to configure TLS connection for kafka broker
	// TLS.enable=true default for TLS.
	// +optional
	TLS *TLS `json:"tls" protobuf:"bytes,4,opt,name=tls"`
	// +optional
	Config string `json:"config,omitempty" protobuf:"bytes,5,opt,name=config"`
}
