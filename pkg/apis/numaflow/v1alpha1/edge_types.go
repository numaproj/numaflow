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

import "fmt"

type Edge struct {
	From string `json:"from" protobuf:"bytes,1,opt,name=from"`
	To   string `json:"to" protobuf:"bytes,2,opt,name=to"`
	// Conditional forwarding, only allowed when "From" is a Source or UDF.
	// +optional
	Conditions *ForwardConditions `json:"conditions" protobuf:"bytes,3,opt,name=conditions"`
	// OnFull specifies the behaviour for the write actions when the inter step buffer is full.
	// There are currently two options, retryUntilSuccess and discardLatest.
	// if not provided, the default value is set to "retryUntilSuccess"
	// +kubebuilder:validation:Enum=retryUntilSuccess;discardLatest
	// +optional
	OnFull *BufferFullWritingStrategy `json:"onFull,omitempty" protobuf:"bytes,4,opt,name=onFull"`
}

// CombinedEdge is a combination of Edge and some other properties such as vertex type, partitions, limits.
// It's used to decorate the fromEdges and toEdges of the generated Vertex objects, so that in the vertex pod,
// it knows the properties of the connected vertices, for example, how many partitioned buffers I should write
// to, what is the write buffer length, etc.
type CombinedEdge struct {
	Edge `json:",inline" protobuf:"bytes,1,opt,name=edge"`
	// From vertex type.
	FromVertexType VertexType `json:"fromVertexType" protobuf:"bytes,2,opt,name=fromVertexType"`
	// The number of partitions of the from vertex, if not provided, the default value is set to "1".
	// +optional
	FromVertexPartitionCount *int32 `json:"fromVertexPartitionCount,omitempty" protobuf:"bytes,3,opt,name=fromVertexPartitionCount"`
	// +optional
	FromVertexLimits *VertexLimits `json:"fromVertexLimits,omitempty" protobuf:"bytes,4,opt,name=fromVertexLimits"`
	// To vertex type.
	ToVertexType VertexType `json:"toVertexType" protobuf:"bytes,5,opt,name=toVertexType"`
	// The number of partitions of the to vertex, if not provided, the default value is set to "1".
	// +optional
	ToVertexPartitionCount *int32 `json:"toVertexPartitionCount,omitempty" protobuf:"bytes,6,opt,name=toVertexPartitionCount"`
	// +optional
	ToVertexLimits *VertexLimits `json:"toVertexLimits,omitempty" protobuf:"bytes,7,opt,name=toVertexLimits"`
}

func (ce CombinedEdge) GetFromVertexPartitions() int {
	if ce.FromVertexPartitionCount == nil || *ce.FromVertexPartitionCount < 1 {
		return 1
	}
	return int(*ce.FromVertexPartitionCount)
}

func (ce CombinedEdge) GetToVertexPartitionCount() int {
	// The controller is responsible for setting the right value of the toPartitions.
	// For example, a non-keyed reduce vertex having partitions > 1 should have toPartitions = 1.
	if ce.ToVertexPartitionCount == nil || *ce.ToVertexPartitionCount < 1 {
		return 1
	}
	return int(*ce.ToVertexPartitionCount)
}

type ForwardConditions struct {
	// Tags used to specify tags for conditional forwarding
	Tags *TagConditions `json:"tags" protobuf:"bytes,1,opt,name=tags"`
}

type LogicOperator string

const (
	LogicOperatorAnd LogicOperator = "and"
	LogicOperatorOr  LogicOperator = "or"
	LogicOperatorNot LogicOperator = "not"
)

type TagConditions struct {
	// Operator specifies the type of operation that should be used for conditional forwarding
	// value could be "and", "or", "not"
	// +kubebuilder:validation:Enum=and;or;not
	// +optional
	Operator *LogicOperator `json:"operator" protobuf:"bytes,1,opt,name=operator"`
	// Values tag values for conditional forwarding
	Values []string `json:"values" protobuf:"bytes,2,rep,name=values"`
}

func (tc TagConditions) GetOperator() LogicOperator {
	if tc.Operator == nil {
		return LogicOperatorOr
	}
	switch *tc.Operator {
	case LogicOperatorOr, LogicOperatorNot, LogicOperatorAnd:
		return *tc.Operator
	default:
		return LogicOperatorOr
	}
}

func (e Edge) BufferFullWritingStrategy() BufferFullWritingStrategy {
	if e.OnFull == nil {
		return RetryUntilSuccess
	}
	switch *e.OnFull {
	case RetryUntilSuccess, DiscardLatest:
		return *e.OnFull
	default:
		return RetryUntilSuccess
	}
}

func (e Edge) GetEdgeName() string {
	return fmt.Sprintf("%s-%s", e.From, e.To)
}

type BufferFullWritingStrategy string

const (
	RetryUntilSuccess BufferFullWritingStrategy = "retryUntilSuccess"
	DiscardLatest     BufferFullWritingStrategy = "discardLatest"
)

func GenerateEdgeBucketName(namespace, pipeline, from, to string) string {
	return fmt.Sprintf("%s-%s-%s-%s", namespace, pipeline, from, to)
}
