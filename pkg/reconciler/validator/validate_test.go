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
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func TestValidateScaleBounds(t *testing.T) {
	tests := []struct {
		name      string
		scale     dfv1.Scale
		wantError string
	}{
		{
			name:  "min and max omitted",
			scale: dfv1.Scale{},
		},
		{
			name:  "min equal to max",
			scale: dfv1.Scale{Min: ptr.To[int32](3), Max: ptr.To[int32](3)},
		},
		{
			name:  "min and max are zero",
			scale: dfv1.Scale{Min: ptr.To[int32](0), Max: ptr.To[int32](0)},
		},
		{
			name:  "min at the default max",
			scale: dfv1.Scale{Min: ptr.To[int32](dfv1.DefaultMaxReplicas)},
		},
		{
			name:      "min above max",
			scale:     dfv1.Scale{Min: ptr.To[int32](10), Max: ptr.To[int32](1)},
			wantError: "max number of replicas should be greater than or equal to min",
		},
		{
			name:      "min above the default max",
			scale:     dfv1.Scale{Min: ptr.To[int32](dfv1.DefaultMaxReplicas + 1)},
			wantError: "max number of replicas should be greater than or equal to min",
		},
		{
			name:      "max below the default min",
			scale:     dfv1.Scale{Max: ptr.To[int32](-1)},
			wantError: "max number of replicas should be greater than or equal to min",
		},
		{
			name:      "min is negative",
			scale:     dfv1.Scale{Min: ptr.To[int32](-1)},
			wantError: "min number of replicas should not be smaller than 0",
		},
		{
			name:      "min above max with autoscaling disabled",
			scale:     dfv1.Scale{Disabled: true, Min: ptr.To[int32](10), Max: ptr.To[int32](1)},
			wantError: "max number of replicas should be greater than or equal to min",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateScaleBounds(tt.scale)
			if tt.wantError == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tt.wantError)
		})
	}
}
