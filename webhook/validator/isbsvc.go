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
	"context"

	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/client-go/kubernetes"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	isbsvccontroller "github.com/numaproj/numaflow/pkg/reconciler/isbsvc"
)

type isbsvcValidator struct {
	client kubernetes.Interface
	isbscv v1alpha1.InterStepBufferServiceInterface

	oldISBService *dfv1.InterStepBufferService
	newISBService *dfv1.InterStepBufferService
}

// returns ISBService validator
func NewISBServiceValidator(client kubernetes.Interface, isbsvc v1alpha1.InterStepBufferServiceInterface, old, new *dfv1.InterStepBufferService) Validator {
	return &isbsvcValidator{client: client, isbscv: isbsvc, oldISBService: old, newISBService: new}
}

func (v *isbsvcValidator) ValidateCreate(ctx context.Context) *admissionv1.AdmissionResponse {
	if err := isbsvccontroller.ValidateInterStepBufferService(v.newISBService); err != nil {
		return DeniedResponse(err.Error())
	}
	return AllowedResponse()
}

func (v *isbsvcValidator) ValidateUpdate(ctx context.Context) *admissionv1.AdmissionResponse {

	if err := isbsvccontroller.ValidateInterStepBufferService(v.newISBService); err != nil {
		return DeniedResponse(err.Error())
	}

	switch {
	case v.oldISBService.Spec.JetStream != nil:
		if v.newISBService.Spec.Redis != nil {
			return DeniedResponse("Can not change ISB Service type from Jetstream to Redis")
		}
	case v.oldISBService.Spec.Redis != nil:
		if v.newISBService.Spec.JetStream != nil {
			return DeniedResponse("Can not change ISB Service type from Redis to Jetstream")
		}
	}

	return AllowedResponse()
}
