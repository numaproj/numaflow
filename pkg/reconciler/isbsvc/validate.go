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

package isbsvc

import (
	"fmt"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// ValidateInterStepBufferService accepts an isbs and performs validation against it
func ValidateInterStepBufferService(isbs *dfv1.InterStepBufferService) error {
	if isbs.Spec.Redis != nil && isbs.Spec.JetStream != nil {
		return fmt.Errorf(`invalid spec: "spec.redis" and "spec.jetstream" can not be defined together`)
	}
	if isbs.Spec.Redis == nil && isbs.Spec.JetStream == nil {
		return fmt.Errorf(`invalid spec: either "spec.redis" or "spec.jetstream" needs to be specified`)
	}
	if isbs.Spec.Redis != nil {
		if isbs.Spec.Redis.Native != nil && isbs.Spec.Redis.External != nil {
			return fmt.Errorf(`"native" and "external" can not be defined together`)
		}
		if isbs.Spec.Redis.Native == nil && isbs.Spec.Redis.External == nil {
			return fmt.Errorf(`either "native" or "external" must be defined`)
		}
		if native := isbs.Spec.Redis.Native; native != nil {
			if native.Version == "" {
				return fmt.Errorf(`invalid spec: "spec.redis.native.version" is not defined`)
			}
		}
	}
	if x := isbs.Spec.JetStream; x != nil {
		if x.Version == "" {
			return fmt.Errorf(`invalid spec: "spec.jetstream.version" is not defined`)
		}
	}
	return nil
}
