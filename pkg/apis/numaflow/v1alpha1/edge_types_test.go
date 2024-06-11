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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func TestEdgeBufferFullWritingStrategy(t *testing.T) {
	tests := []struct {
		name     string
		edge     Edge
		expected BufferFullWritingStrategy
	}{
		{
			name:     "default strategy",
			edge:     Edge{},
			expected: RetryUntilSuccess,
		},
		{
			name:     "retry until success strategy",
			edge:     Edge{OnFull: ptr.To[BufferFullWritingStrategy](RetryUntilSuccess)},
			expected: RetryUntilSuccess,
		},
		{
			name:     "discard latest strategy",
			edge:     Edge{OnFull: ptr.To[BufferFullWritingStrategy](DiscardLatest)},
			expected: DiscardLatest,
		},
		{
			name:     "invalid strategy",
			edge:     Edge{OnFull: ptr.To[BufferFullWritingStrategy]("invalid")},
			expected: RetryUntilSuccess,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.edge.BufferFullWritingStrategy())
		})
	}
}

func TestGenerateEdgeBucketName(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		pipeline  string
		from      string
		to        string
		expected  string
	}{
		{
			name:      "valid inputs",
			namespace: "test-namespace",
			pipeline:  "test-pipeline",
			from:      "source",
			to:        "sink",
			expected:  "test-namespace-test-pipeline-source-sink",
		},
		{
			name:      "empty inputs",
			namespace: "",
			pipeline:  "",
			from:      "",
			to:        "",
			expected:  "---",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, GenerateEdgeBucketName(test.namespace, test.pipeline, test.from, test.to))
		})
	}
}

func TestCombinedEdgeGetFromVertexPartitions(t *testing.T) {
	tests := []struct {
		name     string
		combined CombinedEdge
		expected int
	}{
		{
			name:     "default partition count",
			combined: CombinedEdge{},
			expected: 1,
		},
		{
			name:     "valid partition count",
			combined: CombinedEdge{FromVertexPartitionCount: ptr.To[int32](4)},
			expected: 4,
		},
		{
			name:     "invalid partition count",
			combined: CombinedEdge{FromVertexPartitionCount: ptr.To[int32](-1)},
			expected: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.combined.GetFromVertexPartitions())
		})
	}
}

func TestCombinedEdgeGetToVertexPartitionCount(t *testing.T) {
	tests := []struct {
		name     string
		combined CombinedEdge
		expected int
	}{
		{
			name:     "default partition count",
			combined: CombinedEdge{},
			expected: 1,
		},
		{
			name:     "valid partition count",
			combined: CombinedEdge{ToVertexPartitionCount: ptr.To[int32](4)},
			expected: 4,
		},
		{
			name:     "invalid partition count",
			combined: CombinedEdge{ToVertexPartitionCount: ptr.To[int32](-1)},
			expected: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.combined.GetToVertexPartitionCount())
		})
	}
}
