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

// HealthCodeInfo is used to maintain status codes for vertex level health
type HealthCodeInfo struct {
	Status      string
	Criticality string
}

// newHealthCodeInfo is used to create a new HealthCodeInfo object
func newHealthCodeInfo(status string, criticality string) *HealthCodeInfo {
	return &HealthCodeInfo{
		Status:      status,
		Criticality: criticality,
	}
}
