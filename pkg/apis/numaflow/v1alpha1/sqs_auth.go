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

import corev1 "k8s.io/api/core/v1"

// SQSAuth defines how to authenticate with AWS SQS
type SQSAuth struct {
	// Either Credentials or RoleARN must be specified

	// AWS Credentials
	// +optional
	Credentials *AWSCredentials `json:"credentials,omitempty" protobuf:"bytes,1,opt,name=credentials"`

	// Role ARN to assume
	// +optional
	RoleARN string `json:"roleARN,omitempty" protobuf:"bytes,2,opt,name=roleARN"`
}

// AWSCredentials contains AWS credentials information
type AWSCredentials struct {
	// AccessKeyID is the AWS access key ID
	AccessKeyID *corev1.SecretKeySelector `json:"accessKeyId" protobuf:"bytes,1,name=accessKeyId"`

	// SecretAccessKey is the AWS secret access key
	SecretAccessKey *corev1.SecretKeySelector `json:"secretAccessKey" protobuf:"bytes,2,name=secretAccessKey"`
}
