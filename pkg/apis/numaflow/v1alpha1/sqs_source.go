/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

// SqsSource represents the configuration of an AWS SQS source
type SqsSource struct {
	// AWSRegion is the AWS Region where the SQS queue is located
	AWSRegion string `json:"awsRegion" protobuf:"bytes,1,name=awsRegion"`

	// QueueName is the name of the SQS queue
	QueueName string `json:"queueName" protobuf:"bytes,2,name=queueName"`

	// QueueOwnerAWSAccountID is the queue owner aws account id
	QueueOwnerAWSAccountID string `json:"queueOwnerAWSAccountID" protobuf:"bytes,3,name=queueOwnerAWSAccountID"`

	// VisibilityTimeout is the duration (in seconds) that the received messages are hidden from subsequent
	// retrieve requests after being retrieved by a ReceiveMessage request.
	// Valid values: 0-43200 (12 hours)
	// +optional
	VisibilityTimeout *int32 `json:"visibilityTimeout,omitempty" protobuf:"varint,4,opt,name=visibilityTimeout"`

	// MaxNumberOfMessages is the maximum number of messages to return in a single poll.
	// Valid values: 1-10
	// Defaults to 1
	// +optional
	MaxNumberOfMessages *int32 `json:"maxNumberOfMessages,omitempty" protobuf:"varint,5,opt,name=maxNumberOfMessages"`

	// WaitTimeSeconds is the duration (in seconds) for which the call waits for a message to arrive
	// in the queue before returning. If a message is available, the call returns sooner than WaitTimeSeconds.
	// Valid values: 0-20
	// Defaults to 0 (short polling)
	// +optional
	WaitTimeSeconds *int32 `json:"waitTimeSeconds,omitempty" protobuf:"varint,6,opt,name=waitTimeSeconds"`

	// EndpointURL is the custom endpoint URL for the AWS SQS API.
	// This is useful for testing with localstack or when using VPC endpoints.
	// +optional
	EndpointURL *string `json:"endpointUrl,omitempty" protobuf:"bytes,7,opt,name=endpointUrl"`

	// AttributeNames is a list of attributes that need to be returned along with each message.
	// Valid values: All | Policy | VisibilityTimeout | MaximumMessageSize | MessageRetentionPeriod |
	// ApproximateNumberOfMessages | ApproximateNumberOfMessagesNotVisible | CreatedTimestamp |
	// LastModifiedTimestamp | QueueArn | ApproximateNumberOfMessagesDelayed | DelaySeconds |
	// ReceiveMessageWaitTimeSeconds | RedrivePolicy | FifoQueue | ContentBasedDeduplication |
	// KmsMasterKeyId | KmsDataKeyReusePeriodSeconds | DeduplicationScope | FifoThroughputLimit |
	// RedriveAllowPolicy | SqsManagedSseEnabled
	// +optional
	AttributeNames []string `json:"attributeNames,omitempty" protobuf:"bytes,8,rep,name=attributeNames"`

	// MessageAttributeNames is a list of message attributes that need to be returned along with each message.
	// +optional
	MessageAttributeNames []string `json:"messageAttributeNames,omitempty" protobuf:"bytes,9,rep,name=messageAttributeNames"`

	// AssumeRole contains the configuration for AWS STS assume role.
	// When specified, the SQS client will assume the specified role for authentication.
	// +optional
	AssumeRole *AWSAssumeRole `json:"assumeRole,omitempty" protobuf:"bytes,10,opt,name=assumeRole"`
}
