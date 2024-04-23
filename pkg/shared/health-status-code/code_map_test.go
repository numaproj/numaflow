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

func TestGetHealthCodeInfo(t *testing.T) {
	type args struct {
		code string
	}
	tests := []struct {
		name string
		args args
		want *HealthCodeInfo
	}{
		{
			name: "TestGetHealthCodeInfo",
			args: args{
				code: "V1",
			},
			want: &HealthCodeInfo{
				Status:      "All pods are running",
				Criticality: "Healthy",
			},
		},
		{
			name: "TestGetHealthCodeInfo",
			args: args{
				code: "V2",
			},
			want: &HealthCodeInfo{
				Status:      "Vertex is not in running state",
				Criticality: "Critical",
			},
		},
		{
			name: "TestGetHealthCodeInfo",
			args: args{
				code: "V3",
			},
			want: &HealthCodeInfo{
				Status:      "Vertex is in running but containers are not in running state",
				Criticality: "Warning",
			},
		},
		{
			name: "TestGetHealthCodeInfo",
			args: args{
				code: "V4",
			},
			want: &HealthCodeInfo{
				Status:      "All vertices healthy in the pipeline",
				Criticality: "Healthy",
			},
		},
		{
			name: "TestGetHealthCodeInfo",
			args: args{
				code: "V5",
			},
			want: &HealthCodeInfo{
				Status:      "One or more vertices are unhealthy in the pipeline",
				Criticality: "Warning",
			},
		},
		{
			name: "TestGetHealthCodeInfo",
			args: args{
				code: "V6",
			},
			want: &HealthCodeInfo{
				Status:      "Pipeline is in an unknown state",
				Criticality: "Critical",
			},
		},
		{
			name: "TestGetHealthCodeInfo",
			args: args{
				code: "V7",
			},
			want: &HealthCodeInfo{
				Status:      "Pipeline is paused",
				Criticality: "Healthy",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getHealthCodeInfo(tt.args.code); got.Status != tt.want.Status {
				t.Errorf("GetHealthCodeInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}
