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

// Scale defines the parameters for autoscaling.
type Scale struct {
	// Whether to disable autoscaling.
	// Set to "true" when using Kubernetes HPA or any other 3rd party autoscaling strategies.
	// +optional
	Disabled bool `json:"disabled,omitempty" protobuf:"bytes,1,opt,name=disabled"`
	// Minimum replicas.
	// +optional
	Min *int32 `json:"min,omitempty" protobuf:"varint,2,opt,name=min"`
	// Maximum replicas.
	// +optional
	Max *int32 `json:"max,omitempty" protobuf:"varint,3,opt,name=max"`
	// Lookback seconds to calculate the average pending messages and processing rate.
	// +optional
	LookbackSeconds *uint32 `json:"lookbackSeconds,omitempty" protobuf:"varint,4,opt,name=lookbackSeconds"`
	// Deprecated: Use scaleUpCooldownSeconds and scaleDownCooldownSeconds instead.
	// Cooldown seconds after a scaling operation before another one.
	// +optional
	DeprecatedCooldownSeconds *uint32 `json:"cooldownSeconds,omitempty" protobuf:"varint,5,opt,name=cooldownSeconds"`
	// After scaling down the source vertex to 0, sleep how many seconds before scaling the source vertex back up to peek.
	// +optional
	ZeroReplicaSleepSeconds *uint32 `json:"zeroReplicaSleepSeconds,omitempty" protobuf:"varint,6,opt,name=zeroReplicaSleepSeconds"`
	// TargetProcessingSeconds is used to tune the aggressiveness of autoscaling for source vertices, it measures how fast
	// you want the vertex to process all the pending messages. Typically increasing the value, which leads to lower processing
	// rate, thus less replicas. It's only effective for source vertices.
	// +optional
	TargetProcessingSeconds *uint32 `json:"targetProcessingSeconds,omitempty" protobuf:"varint,7,opt,name=targetProcessingSeconds"`
	// TargetBufferAvailability is used to define the target percentage of the buffer availability.
	// A valid and meaningful value should be less than the BufferUsageLimit defined in the Edge spec (or Pipeline spec), for example, 50.
	// It only applies to UDF and Sink vertices because only they have buffers to read.
	// +optional
	TargetBufferAvailability *uint32 `json:"targetBufferAvailability,omitempty" protobuf:"varint,8,opt,name=targetBufferAvailability"`
	// ReplicasPerScale defines maximum replicas can be scaled up or down at once.
	// The is use to prevent too aggressive scaling operations
	// +optional
	ReplicasPerScale *uint32 `json:"replicasPerScale,omitempty" protobuf:"varint,9,opt,name=replicasPerScale"`
	// ScaleUpCooldownSeconds defines the cooldown seconds after a scaling operation, before a follow-up scaling up.
	// It defaults to the CooldownSeconds if not set.
	// +optional
	ScaleUpCooldownSeconds *uint32 `json:"scaleUpCooldownSeconds,omitempty" protobuf:"varint,10,opt,name=scaleUpCooldownSeconds"`
	// ScaleDownCooldownSeconds defines the cooldown seconds after a scaling operation, before a follow-up scaling down.
	// It defaults to the CooldownSeconds if not set.
	// +optional
	ScaleDownCooldownSeconds *uint32 `json:"scaleDownCooldownSeconds,omitempty" protobuf:"varint,11,opt,name=scaleDownCooldownSeconds"`
}

func (s Scale) GetLookbackSeconds() int {
	if s.LookbackSeconds != nil {
		return int(*s.LookbackSeconds)
	}
	return DefaultLookbackSeconds
}

func (s Scale) GetScaleUpCooldownSeconds() int {
	if s.ScaleUpCooldownSeconds != nil {
		return int(*s.ScaleUpCooldownSeconds)
	}
	if s.DeprecatedCooldownSeconds != nil {
		return int(*s.DeprecatedCooldownSeconds)
	}
	return DefaultCooldownSeconds
}

func (s Scale) GetScaleDownCooldownSeconds() int {
	if s.ScaleDownCooldownSeconds != nil {
		return int(*s.ScaleDownCooldownSeconds)
	}
	if s.DeprecatedCooldownSeconds != nil {
		return int(*s.DeprecatedCooldownSeconds)
	}
	return DefaultCooldownSeconds
}

func (s Scale) GetZeroReplicaSleepSeconds() int {
	if s.ZeroReplicaSleepSeconds != nil {
		return int(*s.ZeroReplicaSleepSeconds)
	}
	return DefaultZeroReplicaSleepSeconds
}

func (s Scale) GetTargetProcessingSeconds() int {
	if s.TargetProcessingSeconds != nil {
		return int(*s.TargetProcessingSeconds)
	}
	return DefaultTargetProcessingSeconds
}

func (s Scale) GetTargetBufferAvailability() int {
	if s.TargetBufferAvailability != nil {
		return int(*s.TargetBufferAvailability)
	}
	return DefaultTargetBufferAvailability
}

func (s Scale) GetReplicasPerScale() int {
	if s.ReplicasPerScale != nil {
		return int(*s.ReplicasPerScale)
	}
	return DefaultReplicasPerScale
}

func (s Scale) GetMinReplicas() int32 {
	if x := s.Min; x == nil || *x < 0 {
		return 0
	} else {
		return *x
	}
}

func (s Scale) GetMaxReplicas() int32 {
	if x := s.Max; x == nil {
		return DefaultMaxReplicas
	} else {
		return *x
	}
}
