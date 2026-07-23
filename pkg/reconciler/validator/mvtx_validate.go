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
	"time"

	cron "github.com/robfig/cron/v3"
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
	if mvtx.GetStreaming() && mvtx.Spec.Source.Kafka != nil {
		return fmt.Errorf("built-in Kafka source is not supported with streaming=true: " +
			"the Kafka source commits offsets cumulatively (highest offset per partition), " +
			"which is incompatible with streaming's per-message out-of-order acknowledgement and may cause data loss; " +
			"disable streaming or use a different source")
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

	if err := validateCronScaling(mvtx.Spec.Scale); err != nil {
		return fmt.Errorf("invalid scale.cron: %w", err)
	}

	return nil
}

func validateCronScaling(scale dfv1.Scale) error {
	if scale.Cron == nil {
		return nil
	}
	if scale.Disabled {
		return fmt.Errorf("cron must not be configured when autoscaling is disabled")
	}
	if scale.Min != nil && *scale.Min < 0 {
		return fmt.Errorf("scale.min must not be negative")
	}
	if scale.Max != nil && *scale.Max < 0 {
		return fmt.Errorf("scale.max must not be negative")
	}
	parentMin := scale.GetMinReplicas()
	parentMax := scale.GetMaxReplicas()
	if parentMin > parentMax {
		return fmt.Errorf("scale.min must not be greater than scale.max")
	}
	if scale.Cron.Timezone == "" {
		return fmt.Errorf("timezone is required")
	}
	if _, err := time.LoadLocation(scale.Cron.Timezone); err != nil {
		return fmt.Errorf("invalid timezone %q: %w", scale.Cron.Timezone, err)
	}
	if len(scale.Cron.Schedules) == 0 {
		return fmt.Errorf("at least one schedule is required")
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	for i, sched := range scale.Cron.Schedules {
		startSchedule, err := parser.Parse(sched.Start)
		if err != nil {
			return fmt.Errorf("schedules[%d].start %q is not a valid cron expression: %w", i, sched.Start, err)
		}
		endSchedule, err := parser.Parse(sched.End)
		if err != nil {
			return fmt.Errorf("schedules[%d].end %q is not a valid cron expression: %w", i, sched.End, err)
		}
		startSpec, startOK := startSchedule.(*cron.SpecSchedule)
		endSpec, endOK := endSchedule.(*cron.SpecSchedule)
		if !startOK || !endOK {
			return fmt.Errorf("schedules[%d]: unsupported cron schedule", i)
		}
		sameSchedule := startSpec.Second == endSpec.Second &&
			startSpec.Minute == endSpec.Minute &&
			startSpec.Hour == endSpec.Hour &&
			startSpec.Dom == endSpec.Dom &&
			startSpec.Month == endSpec.Month &&
			startSpec.Dow == endSpec.Dow
		if sameSchedule {
			return fmt.Errorf("schedules[%d]: start and end must not be identical", i)
		}
		if startSpec.Dom != endSpec.Dom || startSpec.Month != endSpec.Month || startSpec.Dow != endSpec.Dow {
			return fmt.Errorf("schedules[%d]: start and end day-of-month, month, and day-of-week must match", i)
		}
		if sched.Min == nil {
			return fmt.Errorf("schedules[%d]: min is required", i)
		}
		if sched.Max == nil {
			return fmt.Errorf("schedules[%d]: max is required", i)
		}
		if *sched.Min < 0 {
			return fmt.Errorf("schedules[%d]: min must not be negative", i)
		}
		if *sched.Max < 0 {
			return fmt.Errorf("schedules[%d]: max must not be negative", i)
		}
		if *sched.Min > *sched.Max {
			return fmt.Errorf("schedules[%d]: min (%d) must not be greater than max (%d)", i, *sched.Min, *sched.Max)
		}
		if *sched.Min < parentMin {
			return fmt.Errorf("schedules[%d]: min must not be less than scale.min", i)
		}
		if *sched.Max > parentMax {
			return fmt.Errorf("schedules[%d]: max must not be greater than scale.max", i)
		}
	}
	return nil
}
