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
	"k8s.io/apimachinery/pkg/util/intstr"
)

// UpdateStrategy indicates the strategy that the
// controller will use to perform updates for Vertex or MonoVertex.
type UpdateStrategy struct {
	// Type indicates the type of the StatefulSetUpdateStrategy.
	// Default is RollingUpdate.
	// +optional
	Type UpdateStrategyType `json:"type,omitempty" protobuf:"bytes,1,opt,name=type,casttype=UpdateStrategyType"`
	// RollingUpdate is used to communicate parameters when Type is RollingUpdateStrategy.
	// +optional
	RollingUpdate *RollingUpdateStrategy `json:"rollingUpdate,omitempty" protobuf:"bytes,2,opt,name=rollingUpdate"`
}

func (us UpdateStrategy) GetUpdateStrategyType() UpdateStrategyType {
	switch us.Type {
	case RollingUpdateStrategyType:
		return us.Type
	default:
		return RollingUpdateStrategyType // We only support RollingUpdateStrategyType for now.
	}
}

func (us UpdateStrategy) GetRollingUpdateStrategy() RollingUpdateStrategy {
	if us.RollingUpdate == nil {
		return RollingUpdateStrategy{}
	}
	return *us.RollingUpdate
}

// UpdateStrategyType is a string enumeration type that enumerates
// all possible update strategies.
// +enum
type UpdateStrategyType string

const (
	RollingUpdateStrategyType UpdateStrategyType = "RollingUpdate"
)

// RollingUpdateStrategy is used to communicate parameter for RollingUpdateStrategyType.
type RollingUpdateStrategy struct {
	// The maximum number of pods that can be unavailable during the update.
	// Value can be an absolute number (ex: 5) or a percentage of desired pods (ex: 10%).
	// Absolute number is calculated from percentage by rounding down.
	// Defaults to 25%.
	// Example: when this is set to 30%, the old pods can be scaled down to 70% of desired pods
	// immediately when the rolling update starts. Once new pods are ready, old pods
	// can be scaled down further, followed by scaling up the new pods, ensuring
	// that the total number of pods available at all times during the update is at
	// least 70% of desired pods.
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty" protobuf:"bytes,1,opt,name=maxUnavailable"`
}

func (rus RollingUpdateStrategy) GetMaxUnavailable() intstr.IntOrString {
	if rus.MaxUnavailable == nil {
		return intstr.FromString("25%") // Default value is 25%.
	}
	return *rus.MaxUnavailable
}
