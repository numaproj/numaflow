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

package scaling

import (
	"fmt"
	"time"

	cron "github.com/robfig/cron/v3"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// ParsedCronSchedule holds a parsed start/end cron schedule pair.
type ParsedCronSchedule struct {
	Schedule dfv1.CronSchedule
	Start    cron.Schedule
	End      cron.Schedule
}

// IsActiveAt reports whether time t falls within this cron schedule window.
func (p *ParsedCronSchedule) IsActiveAt(t time.Time) bool {
	return p.End.Next(t).Before(p.Start.Next(t))
}

// ParseCronSchedules parses a CronScheduling spec into a slice of ParsedCronSchedule.
func ParseCronSchedules(cronScheduling *dfv1.CronScheduling) ([]ParsedCronSchedule, error) {
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	result := make([]ParsedCronSchedule, 0, len(cronScheduling.Schedules))
	for _, schedule := range cronScheduling.Schedules {
		start, err := parser.Parse(schedule.Start)
		if err != nil {
			return nil, fmt.Errorf("invalid cron start expression %q: %w", schedule.Start, err)
		}
		end, err := parser.Parse(schedule.End)
		if err != nil {
			return nil, fmt.Errorf("invalid cron end expression %q: %w", schedule.End, err)
		}
		result = append(result, ParsedCronSchedule{Schedule: schedule, Start: start, End: end})
	}
	return result, nil
}

// EffectiveScaleBoundsAt returns the effective scale bounds (minReplicas, maxReplicas, active) for a given Scale spec at time at.
func EffectiveScaleBoundsAt(scale dfv1.Scale, parsed []ParsedCronSchedule, at time.Time) (int32, int32, bool) {
	minReplicas := scale.GetMinReplicas()
	maxReplicas := scale.GetMaxReplicas()
	if scale.Cron == nil || len(parsed) == 0 {
		return minReplicas, maxReplicas, false
	}
	location, err := time.LoadLocation(scale.Cron.GetTimezone())
	if err != nil {
		return minReplicas, maxReplicas, false
	}
	at = at.In(location)
	for i := range parsed {
		if parsed[i].IsActiveAt(at) {
			if parsed[i].Schedule.Min != nil {
				minReplicas = *parsed[i].Schedule.Min
			}
			if parsed[i].Schedule.Max != nil {
				maxReplicas = *parsed[i].Schedule.Max
			}
			return minReplicas, maxReplicas, true
		}
	}
	return minReplicas, maxReplicas, false
}

// CalculateEffectiveReplicas clamps desiredReplicas to scale's effective bounds
// at time at: the active cron window's min/max when one applies, otherwise the
// base min/max. Autoscaling-disabled specs are not clamped.
func CalculateEffectiveReplicas(scale dfv1.Scale, desiredReplicas int, at time.Time) int {
	if scale.Disabled {
		return desiredReplicas
	}
	var parsed []ParsedCronSchedule
	if scale.Cron != nil {
		// Parsing errors are already rejected at admission time by the
		// validating webhook; if one somehow slips through, fall back to
		// base bounds rather than failing pod reconciliation.
		if p, err := ParseCronSchedules(scale.Cron); err == nil {
			parsed = p
		}
	}
	minReplicas, maxReplicas, _ := EffectiveScaleBoundsAt(scale, parsed, at)
	if desiredReplicas < int(minReplicas) {
		return int(minReplicas)
	}
	if desiredReplicas > int(maxReplicas) {
		return int(maxReplicas)
	}
	return desiredReplicas
}
