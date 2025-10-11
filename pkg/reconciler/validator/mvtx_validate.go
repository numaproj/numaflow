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

	"k8s.io/apimachinery/pkg/util/intstr"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func ValidateMonoVertex(mvtx *dfv1.MonoVertex) error {
	if mvtx == nil {
		return fmt.Errorf("nil MonoVertex")
	}
	if errs := k8svalidation.IsDNS1035Label(mvtx.Name); len(errs) > 0 {
		return fmt.Errorf("invalid mvtx name %q, %v", mvtx.Name, errs)
	}
	if mvtx.Spec.Source == nil {
		return fmt.Errorf("source is not defined")
	}
	if mvtx.Spec.Source.Serving != nil {
		return fmt.Errorf("serving source is not supported with Monovertex yet")
	}
	if err := validateSource(*mvtx.Spec.Source); err != nil {
		return fmt.Errorf("invalid source: %w", err)
	}
	if mvtx.Spec.Sink == nil {
		return fmt.Errorf("sink is not defined")
	}
	if err := validateSink(*mvtx.Spec.Sink); err != nil {
		return fmt.Errorf("invalid sink: %w", err)
	}
	for _, ic := range mvtx.Spec.InitContainers {
		if isReservedContainerName(ic.Name) {
			return fmt.Errorf("invalid init container name: %q is reserved for containers created by numaflow", ic.Name)
		}
	}

	if mvtx.Spec.UDF != nil {
		if mvtx.Spec.UDF.GroupBy != nil {
			return fmt.Errorf("invalid udf: groupBy/reduce is not supported in monovertex")
		} else if err := validateUDF(*mvtx.Spec.UDF); err != nil {
			return fmt.Errorf("invalid udf: %w", err)
		}
	}

	for _, sc := range mvtx.Spec.Sidecars {
		if isReservedContainerName(sc.Name) {
			return fmt.Errorf("invalid sidecar container name: %q is reserved for containers created by numaflow", sc.Name)
		}
	}
	// Validate the update strategy.
	maxUvail := mvtx.Spec.UpdateStrategy.GetRollingUpdateStrategy().GetMaxUnavailable()
	_, err := intstr.GetScaledValueFromIntOrPercent(&maxUvail, 1, true) // maxUnavailable should be an interger or a percentage in string
	if err != nil {
		return fmt.Errorf("invalid maxUnavailable: %w", err)
	}
	return nil
}
