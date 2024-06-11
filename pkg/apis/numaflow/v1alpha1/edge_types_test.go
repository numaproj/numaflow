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

func Test_EdgeBufferFullWritingStrategy(t *testing.T) {
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

func Test_GenerateEdgeBucketName(t *testing.T) {
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

func Test_CombinedEdgeGetFromVertexPartitions(t *testing.T) {
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

func Test_CombinedEdgeGetToVertexPartitionCount(t *testing.T) {
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

func Test_EdgeGetEdgeName(t *testing.T) {
	tests := []struct {
		name     string
		edge     Edge
		expected string
	}{
		{
			name:     "valid edge",
			edge:     Edge{From: "source", To: "sink"},
			expected: "source-sink",
		},
		{
			name:     "empty from",
			edge:     Edge{From: "", To: "sink"},
			expected: "-sink",
		},
		{
			name:     "empty to",
			edge:     Edge{From: "source", To: ""},
			expected: "source-",
		},
		{
			name:     "empty from and to",
			edge:     Edge{From: "", To: ""},
			expected: "-",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.edge.GetEdgeName())
		})
	}
}
func TestTagConditionsGetOperator(t *testing.T) {
	tests := []struct {
		name     string
		tc       TagConditions
		expected LogicOperator
	}{
		{
			name:     "nil operator",
			tc:       TagConditions{},
			expected: LogicOperatorOr,
		},
		{
			name:     "or operator",
			tc:       TagConditions{Operator: ptr.To[LogicOperator](LogicOperatorOr)},
			expected: LogicOperatorOr,
		},
		{
			name:     "not operator",
			tc:       TagConditions{Operator: ptr.To[LogicOperator](LogicOperatorNot)},
			expected: LogicOperatorNot,
		},
		{
			name:     "and operator",
			tc:       TagConditions{Operator: ptr.To[LogicOperator](LogicOperatorAnd)},
			expected: LogicOperatorAnd,
		},
		{
			name:     "invalid operator",
			tc:       TagConditions{Operator: ptr.To[LogicOperator]("invalid")},
			expected: LogicOperatorOr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.tc.GetOperator())
		})
	}
}
