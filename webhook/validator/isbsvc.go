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
	isbsvccontroller "github.com/numaproj/numaflow/pkg/reconciler/isbsvc"
)

type isbsvcValidator struct {
	oldISBService *dfv1.InterStepBufferService
	newISBService *dfv1.InterStepBufferService
}

func NewISBServiceValidator(old, new *dfv1.InterStepBufferService) Validator {
	return &isbsvcValidator{oldISBService: old, newISBService: new}
}

func (v *isbsvcValidator) ValidateCreate(_ context.Context) *admissionv1.AdmissionResponse {
	if err := isbsvccontroller.ValidateInterStepBufferService(v.newISBService); err != nil {
		return DeniedResponse(err.Error())
	}
	return AllowedResponse()
}

func (v *isbsvcValidator) ValidateUpdate(_ context.Context) *admissionv1.AdmissionResponse {
	// check the new ISB Service is valid
	if err := isbsvccontroller.ValidateInterStepBufferService(v.newISBService); err != nil {
		return DeniedResponse(err.Error())
	}
	switch {
	case v.oldISBService.Spec.JetStream != nil:
		// check the type of ISB Service is not changed
		if v.newISBService.Spec.Redis != nil {
			return DeniedResponse("can not change ISB Service type from Jetstream to Redis")
		}
		// check the persistence of ISB Service is not changed
		if !equality.Semantic.DeepEqual(v.oldISBService.Spec.JetStream.Persistence, v.newISBService.Spec.JetStream.Persistence) {
			return DeniedResponse("can not change persistence of Jetstream ISB Service")
		}
	case v.oldISBService.Spec.Redis != nil:
		// check the type of ISB Service is not changed
		if v.newISBService.Spec.JetStream != nil {
			return DeniedResponse("can not change ISB Service type from Redis to Jetstream")
		}
		// nil check for Redis Native, if one of them is nil and the other is not, it is NOT allowed
		if oldRedisNative, newRedisNative := v.oldISBService.Spec.Redis.Native, v.newISBService.Spec.Redis.Native; oldRedisNative != nil && newRedisNative == nil {
			return DeniedResponse("can not remove Redis Native from Redis ISB Service")
		} else if oldRedisNative == nil && newRedisNative != nil {
			return DeniedResponse("can not add Redis Native to Redis ISB Service")
		}
		// check the persistence of ISB Service is not changed
		if oldRedisNative, newRedisNative := v.oldISBService.Spec.Redis.Native, v.newISBService.Spec.Redis.Native; oldRedisNative != nil && newRedisNative != nil && !equality.Semantic.DeepEqual(oldRedisNative.Persistence, newRedisNative.Persistence) {
			return DeniedResponse("can not change persistence of Redis ISB Service")
		}
	}
	return AllowedResponse()
}
