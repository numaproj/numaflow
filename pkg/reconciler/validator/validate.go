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

package validator

import (
	"fmt"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// validateScaleBounds checks the min and max replicas of a scale spec.
func validateScaleBounds(scale dfv1.Scale) error {
	min, max := int32(0), int32(dfv1.DefaultMaxReplicas)
	if scale.Min != nil {
		min = *scale.Min
	}
	if scale.Max != nil {
		max = *scale.Max
	}
	if min < 0 {
		return fmt.Errorf("min number of replicas should not be smaller than 0")
	}
	if min > max {
		return fmt.Errorf("max number of replicas should be greater than or equal to min")
	}
	return nil
}
