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

	k8svalidation "k8s.io/apimachinery/pkg/util/validation"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// ValidateInterStepBufferService accepts an ISB Service and performs validation against it
func ValidateInterStepBufferService(isbsvc *dfv1.InterStepBufferService) error {
	if isbsvc == nil {
		return fmt.Errorf("nil ISB Service")
	}
	if errs := k8svalidation.IsDNS1035Label(isbsvc.Name); len(errs) > 0 {
		return fmt.Errorf("invalid ISB Service name %q, %v", isbsvc.Name, errs)
	}
	if isbsvc.Spec.JetStream == nil {
		return fmt.Errorf(`invalid spec: "spec.jetstream" needs to be specified`)
	}
	if x := isbsvc.Spec.JetStream; x != nil {
		if x.Version == "" {
			return fmt.Errorf(`invalid spec: "spec.jetstream.version" is not defined`)
		}
		if x.Replicas != nil && (*x.Replicas == 2 || *x.Replicas <= 0) {
			return fmt.Errorf(`invalid spec: min value for "spec.jetstream.replicas" is 1 and can't be 2`)
		}
	}
	return nil
}
