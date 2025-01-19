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
	"encoding/json"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

type Validator interface {
	ValidateCreate(context.Context) *admissionv1.AdmissionResponse
	ValidateUpdate(context.Context) *admissionv1.AdmissionResponse
}

// GetValidator returns a Validator instance
func GetValidator(ctx context.Context, NumaClient v1alpha1.NumaflowV1alpha1Interface, kind metav1.GroupVersionKind, oldBytes []byte, newBytes []byte) (Validator, error) {
	log := logging.FromContext(ctx)
	switch kind.Kind {
	case dfv1.ISBGroupVersionKind.Kind:
		var newSpec *dfv1.InterStepBufferService
		if len(newBytes) > 0 {
			newSpec = &dfv1.InterStepBufferService{}
			if err := json.Unmarshal(newBytes, newSpec); err != nil {
				log.Errorf("Could not unmarshal new raw object: %v", err)
				return nil, err
			}
		}
		var oldSpec *dfv1.InterStepBufferService
		if len(oldBytes) > 0 {
			oldSpec = &dfv1.InterStepBufferService{}
			if err := json.Unmarshal(oldBytes, oldSpec); err != nil {
				log.Errorf("Could not unmarshal old raw object: %v", err)
				return nil, err
			}
		}
		return NewISBServiceValidator(oldSpec, newSpec), nil
	case dfv1.PipelineGroupVersionKind.Kind:
		var newSpec *dfv1.Pipeline
		if len(newBytes) > 0 {
			newSpec = &dfv1.Pipeline{}
			if err := json.Unmarshal(newBytes, newSpec); err != nil {
				log.Errorf("Could not unmarshal new raw object: %v", err)
				return nil, err
			}
		}
		var oldSpec *dfv1.Pipeline
		if len(oldBytes) > 0 {
			oldSpec = &dfv1.Pipeline{}
			if err := json.Unmarshal(oldBytes, oldSpec); err != nil {
				log.Errorf("Could not unmarshal old raw object: %v", err)
				return nil, err
			}
		}
		isbSvcClient := NumaClient.InterStepBufferServices(newSpec.Namespace)
		return NewPipelineValidator(isbSvcClient, oldSpec, newSpec), nil
	default:
		return nil, fmt.Errorf("unrecognized kind: %v", kind)
	}
}

// DeniedResponse constructs a denied AdmissionResponse
func DeniedResponse(reason string, args ...interface{}) *admissionv1.AdmissionResponse {
	if len(args) > 0 {
		reason = fmt.Sprintf(reason, args)
	}
	result := apierrors.NewBadRequest(reason).Status()
	return &admissionv1.AdmissionResponse{
		Result:  &result,
		Allowed: false,
	}
}

// AllowedResponse constructs an allowed AdmissionResponse
func AllowedResponse() *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: true,
	}
}
