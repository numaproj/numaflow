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
	"k8s.io/apimachinery/pkg/api/equality"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler/validator"
)

type isbsvcValidator struct {
	oldISBService *dfv1.InterStepBufferService
	newISBService *dfv1.InterStepBufferService
}

func NewISBServiceValidator(old, new *dfv1.InterStepBufferService) Validator {
	return &isbsvcValidator{oldISBService: old, newISBService: new}
}

func (v *isbsvcValidator) ValidateCreate(_ context.Context) *admissionv1.AdmissionResponse {
	if err := validator.ValidateInterStepBufferService(v.newISBService); err != nil {
		return DeniedResponse(err.Error())
	}
	return AllowedResponse()
}

func (v *isbsvcValidator) ValidateUpdate(_ context.Context) *admissionv1.AdmissionResponse {
	// check the new ISB Service is valid
	if err := validator.ValidateInterStepBufferService(v.newISBService); err != nil {
		return DeniedResponse(err.Error())
	}
	// chck if the instance annotation is changed
	if v.oldISBService.GetAnnotations()[dfv1.KeyInstance] != v.newISBService.GetAnnotations()[dfv1.KeyInstance] {
		return DeniedResponse("cannot update instance annotation " + dfv1.KeyInstance)
	}
	// check the type of ISB Service is not changed
	if v.oldISBService.GetType() != v.newISBService.GetType() {
		return DeniedResponse("can not change ISB Service type")
	}
	switch v.newISBService.GetType() {
	case dfv1.ISBSvcTypeJetStream:
		// check the persistence of ISB Service is not changed
		if !equality.Semantic.DeepEqual(v.oldISBService.Spec.JetStream.Persistence, v.newISBService.Spec.JetStream.Persistence) {
			return DeniedResponse("can not change persistence of Jetstream ISB Service")
		}
	default:
	}
	return AllowedResponse()
}
