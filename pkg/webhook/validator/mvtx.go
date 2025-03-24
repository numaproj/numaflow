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
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler/validator"
)

type mvtxValidator struct {
	oldMvtx *dfv1.MonoVertex
	newMvtx *dfv1.MonoVertex
}

func NewMonoVertexValidator(old, new *dfv1.MonoVertex) Validator {
	return &mvtxValidator{
		oldMvtx: old,
		newMvtx: new,
	}
}

func (v *mvtxValidator) ValidateCreate(ctx context.Context) *admissionv1.AdmissionResponse {
	if err := validator.ValidateMonoVertex(v.newMvtx); err != nil {
		return DeniedResponse(err.Error())
	}
	return AllowedResponse()
}

func (v *mvtxValidator) ValidateUpdate(_ context.Context) *admissionv1.AdmissionResponse {
	if v.oldMvtx == nil {
		return DeniedResponse("old MonoVertex spec is nil")
	}
	// check that the new MonoVertex spec is valid
	if err := validator.ValidateMonoVertex(v.newMvtx); err != nil {
		return DeniedResponse(fmt.Sprintf("new MonoVertex spec is invalid: %s", err.Error()))
	}
	return AllowedResponse()
}
