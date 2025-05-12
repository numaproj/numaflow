package v1alpha1

type SqsSink struct {
	// AWSRegion is the AWS Region where the SQS queue is located
	AWSRegion string `json:"awsRegion" protobuf:"bytes,1,name=awsRegion"`

	// QueueName is the name of the SQS queue
	QueueName string `json:"queueName" protobuf:"bytes,2,name=queueName"`

	// QueueOwnerAWSAccountID is the queue owner aws account id
	QueueOwnerAWSAccountID string `json:"queueOwnerAWSAccountID" protobuf:"bytes,3,name=queueOwnerAWSAccountID"`
}
