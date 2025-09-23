/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAWSAssumeRole_GetSessionName(t *testing.T) {
	tests := []struct {
		name          string
		assumeRole    *AWSAssumeRole
		defaultPrefix string
		expected      string
	}{
		{
			name: "with session name specified",
			assumeRole: &AWSAssumeRole{
				RoleARN:     "arn:aws:iam::123456789012:role/test-role",
				SessionName: stringPtr("custom-session"),
			},
			defaultPrefix: "numaflow-service-client",
			expected:      "custom-session",
		},
		{
			name: "without session name specified",
			assumeRole: &AWSAssumeRole{
				RoleARN: "arn:aws:iam::123456789012:role/test-role",
			},
			defaultPrefix: "numaflow-service-client",
			expected:      "numaflow-service-client",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.assumeRole.GetSessionName(tt.defaultPrefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAWSAssumeRole_GetDurationSeconds(t *testing.T) {
	tests := []struct {
		name       string
		assumeRole *AWSAssumeRole
		expected   int32
	}{
		{
			name: "with duration specified",
			assumeRole: &AWSAssumeRole{
				RoleARN:         "arn:aws:iam::123456789012:role/test-role",
				DurationSeconds: int32Ptr(7200),
			},
			expected: 7200,
		},
		{
			name: "without duration specified",
			assumeRole: &AWSAssumeRole{
				RoleARN: "arn:aws:iam::123456789012:role/test-role",
			},
			expected: 3600,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.assumeRole.GetDurationSeconds()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAWSAssumeRole_IsValid(t *testing.T) {
	tests := []struct {
		name       string
		assumeRole *AWSAssumeRole
		expected   bool
	}{
		{
			name: "valid configuration",
			assumeRole: &AWSAssumeRole{
				RoleARN: "arn:aws:iam::123456789012:role/test-role",
			},
			expected: true,
		},
		{
			name:       "nil configuration",
			assumeRole: nil,
			expected:   false,
		},
		{
			name: "missing role ARN",
			assumeRole: &AWSAssumeRole{
				SessionName: stringPtr("test-session"),
			},
			expected: false,
		},
		{
			name: "invalid duration - too short",
			assumeRole: &AWSAssumeRole{
				RoleARN:         "arn:aws:iam::123456789012:role/test-role",
				DurationSeconds: int32Ptr(300), // Less than 900
			},
			expected: false,
		},
		{
			name: "invalid duration - too long",
			assumeRole: &AWSAssumeRole{
				RoleARN:         "arn:aws:iam::123456789012:role/test-role",
				DurationSeconds: int32Ptr(50000), // More than 43200
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.assumeRole.IsValid()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Helper functions
func stringPtr(s string) *string {
	return &s
}

func int32Ptr(i int32) *int32 {
	return &i
}
