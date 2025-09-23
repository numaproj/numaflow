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

// AWSAssumeRole contains the configuration for AWS STS assume role authentication
// This can be used with any AWS service (SQS, S3, DynamoDB, etc.)
type AWSAssumeRole struct {
	// RoleARN is the Amazon Resource Name (ARN) of the role to assume.
	// This is a required field when assume role is enabled.
	// Example: "arn:aws:iam::123456789012:role/CrossAccount-Service-Role"
	RoleARN string `json:"roleArn" protobuf:"bytes,1,name=roleArn"`

	// SessionName is an identifier for the assumed role session.
	// This appears in AWS CloudTrail logs to help identify the source of API calls.
	// If not specified, a default session name will be generated based on the service context.
	// +optional
	SessionName *string `json:"sessionName,omitempty" protobuf:"bytes,2,opt,name=sessionName"`

	// DurationSeconds is the duration (in seconds) of the role session.
	// Valid values: 900-43200 (15 minutes to 12 hours)
	// Defaults to 3600 (1 hour) if not specified.
	// The actual session duration is constrained by the maximum session duration
	// setting of the IAM role being assumed.
	// +optional
	DurationSeconds *int32 `json:"durationSeconds,omitempty" protobuf:"varint,3,opt,name=durationSeconds"`

	// ExternalID is a unique identifier that might be required when you assume a role
	// in another account. This is commonly used as an additional security measure
	// for cross-account role access.
	// +optional
	ExternalID *string `json:"externalID,omitempty" protobuf:"bytes,4,opt,name=externalID"`

	// Policy is an IAM policy document (JSON string) that you want to use as an inline session policy.
	// This parameter is optional. When specified, the session permissions are the intersection of
	// the IAM role's identity-based policy and the session policies.
	// This allows further restriction of permissions for the specific service operations.
	// +optional
	Policy *string `json:"policy,omitempty" protobuf:"bytes,5,opt,name=policy"`

	// PolicyARNs is a list of Amazon Resource Names (ARNs) of IAM managed policies
	// that you want to use as managed session policies.
	// The policies must exist in the same account as the role.
	// This allows attaching existing managed policies to further restrict session permissions.
	// +optional
	PolicyARNs []string `json:"policyArns,omitempty" protobuf:"bytes,6,rep,name=policyArns"`
}

// GetSessionName returns the session name or a default value
func (ar *AWSAssumeRole) GetSessionName(defaultPrefix string) string {
	if ar.SessionName != nil && *ar.SessionName != "" {
		return *ar.SessionName
	}
	return defaultPrefix
}

// GetDurationSeconds returns the duration in seconds or a default value
func (ar *AWSAssumeRole) GetDurationSeconds() int32 {
	if ar.DurationSeconds != nil {
		return *ar.DurationSeconds
	}
	return 3600 // Default to 1 hour
}

// IsValid performs basic validation of the assume role configuration
func (ar *AWSAssumeRole) IsValid() bool {
	if ar == nil {
		return false
	}

	// RoleARN is required
	if ar.RoleARN == "" {
		return false
	}

	// Validate duration if specified
	if ar.DurationSeconds != nil {
		duration := *ar.DurationSeconds
		if duration < 900 || duration > 43200 { // 15 minutes to 12 hours
			return false
		}
	}

	return true
}
