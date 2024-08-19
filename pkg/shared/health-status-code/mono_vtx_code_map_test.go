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

package health_status_code

import "testing"

func TestHealthCodeInformation(t *testing.T) {
	tests := []struct {
		code                string
		expectedCriticality string
		expectedStatus      string
	}{
		{"M1", "Healthy", "Mono Vertex is healthy"},
		{"M2", "Critical", "Mono Vertex is in a critical state"},
		{"M3", "Warning", "Mono Vertex is in a warning state"},
		{"M4", "Critical", "Mono Vertex is in an unknown state"},
		{"M5", "Warning", "Mono Vertex is in a paused state"},
	}

	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			info, exists := monoVtxResourceMap[test.code]
			if !exists {
				t.Errorf("Health code %s does not exist in map", test.code)
			}

			if info.Status != test.expectedStatus {
				t.Errorf("Wrong state for %s: got %s, want %s", test.code, info.Status, test.expectedStatus)
			}

			if info.Criticality != test.expectedCriticality {
				t.Errorf("Wrong description for %s: got %s, want %s", test.code, info.Criticality, test.expectedCriticality)
			}
		})
	}
}
