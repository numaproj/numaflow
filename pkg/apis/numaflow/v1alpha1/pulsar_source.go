package v1alpha1

type PulsarDeadLetterPolicy struct {
	// Topic where messages exceeding the max redelivery count will be sent.
	Topic string `json:"topic" protobuf:"bytes,1,name=topic"`

	// Maximum number of redelivery attempts before routing to the dead letter topic.
	MaxRedelivery uint32 `json:"maxRedelivery" protobuf:"varint,2,opt,name=maxRedelivery"`
}

type PulsarSource struct {
	ServerAddr       string `json:"serverAddr" protobuf:"bytes,1,name=server_addr"`
	Topic            string `json:"topic" protobuf:"bytes,2,name=topic"`
	ConsumerName     string `json:"consumerName" protobuf:"bytes,3,name=consumerName"`
	SubscriptionName string `json:"subscriptionName" protobuf:"bytes,4,name=subscriptionName"`

	// Maximum number of messages that are in not yet acked state. Once this limit is crossed, futher read requests will return empty list.
	MaxUnack uint32 `json:"maxUnack,omitempty" protobuf:"bytes,5,opt,name=maxUnack"`

	// Auth information
	// +optional
	Auth *PulsarAuth `json:"auth,omitempty" protobuf:"bytes,6,opt,name=auth"`

	// Consumer level dead letter policy.
	// +optional
	DeadLetterPolicy *PulsarDeadLetterPolicy `json:"deadLetterPolicy,omitempty" protobuf:"bytes,7,opt,name=deadLetterPolicy"`

	// TLS configuration for the Pulsar client, e.g. to trust a custom/self-signed
	// broker CA. Only server-authentication (one-way TLS) is supported today:
	// CACertSecret is honored, but CertSecret/KeySecret (mutual TLS) are not,
	// since the underlying pulsar-rs client does not currently support presenting
	// a client certificate.
	// +optional
	TLS *TLS `json:"tls,omitempty" protobuf:"bytes,8,opt,name=tls"`
}
