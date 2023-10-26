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

package v1

import "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

const (
	ISBServiceStatusHealthy  = "healthy"
	ISBServiceStatusCritical = "critical"
	ISBServiceStatusWarning  = "warning"
	ISBServiceStatusInactive = "inactive"
)

// ISBServices is a list of InterStepBufferServices
type ISBServices []ISBService

// ISBService gives the summarized information of an InterStepBufferService
type ISBService struct {
	Name       string                          `json:"name"`
	Status     string                          `json:"status"`
	ISBService v1alpha1.InterStepBufferService `json:"isbService"`
}

// NewISBService creates a new ISBService object with the given specifications
func NewISBService(status string, isb *v1alpha1.InterStepBufferService) ISBService {
	return ISBService{
		Name:       isb.Name,
		Status:     status,
		ISBService: *isb,
	}
}
