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

package v1alpha1

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"
)

func TestCronSchedule_IsActiveAt(t *testing.T) {
	tests := []struct {
		name     string
		start    string
		end      string
		t        time.Time
		expected bool
	}{
		{
			name:     "within_window_9am_to_6pm",
			start:    "0 9 * * 1-5",
			end:      "0 18 * * 1-5",
			t:        time.Date(2026, 7, 23, 14, 30, 0, 0, time.UTC), // Wed 2:30 PM
			expected: true,
		},
		{
			name:     "outside_window_after_6pm",
			start:    "0 9 * * 1-5",
			end:      "0 18 * * 1-5",
			t:        time.Date(2026, 7, 23, 20, 0, 0, 0, time.UTC), // Wed 8 PM
			expected: false,
		},
		{
			name:     "outside_window_before_9am",
			start:    "0 9 * * 1-5",
			end:      "0 18 * * 1-5",
			t:        time.Date(2026, 7, 23, 7, 0, 0, 0, time.UTC), // Wed 7 AM
			expected: false,
		},
		{
			name:     "cross_midnight_window_active",
			start:    "0 22 * * *",
			end:      "0 6 * * *",
			t:        time.Date(2026, 7, 23, 2, 0, 0, 0, time.UTC), // 2 AM
			expected: true,
		},
		{
			name:     "cross_midnight_window_inactive",
			start:    "0 22 * * *",
			end:      "0 6 * * *",
			t:        time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC), // noon
			expected: false,
		},
		{
			name:     "exactly_at_start",
			start:    "0 9 * * *",
			end:      "0 18 * * *",
			t:        time.Date(2026, 7, 23, 9, 0, 0, 0, time.UTC), // exactly 9 AM
			expected: true,
		},
		{
			name:     "exactly_at_end",
			start:    "0 9 * * *",
			end:      "0 18 * * *",
			t:        time.Date(2026, 7, 23, 18, 0, 0, 0, time.UTC), // exactly 6 PM
			expected: false,
		},
		{
			name:     "invalid_start_cron",
			start:    "invalid",
			end:      "0 18 * * *",
			t:        time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
			expected: false,
		},
		{
			name:     "invalid_end_cron",
			start:    "0 9 * * *",
			end:      "bad expression",
			t:        time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC),
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cs := CronSchedule{Start: tc.start, End: tc.end}
			got := cs.IsActiveAt(tc.t)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func Test_Scale_Parameters(t *testing.T) {
	s := Scale{}
	assert.Equal(t, int32(0), s.GetMinReplicas())
	assert.Equal(t, int32(DefaultMaxReplicas), s.GetMaxReplicas())
	assert.Equal(t, DefaultCooldownSeconds, s.GetScaleUpCooldownSeconds())
	assert.Equal(t, DefaultCooldownSeconds, s.GetScaleDownCooldownSeconds())
	assert.Equal(t, DefaultLookbackSeconds, s.GetLookbackSeconds())
	assert.Equal(t, DefaultReplicasPerScale, s.GetReplicasPerScaleUp())
	assert.Equal(t, DefaultReplicasPerScale, s.GetReplicasPerScaleDown())
	assert.Equal(t, DefaultTargetBufferAvailability, s.GetTargetBufferAvailability())
	assert.Equal(t, DefaultTargetProcessingSeconds, s.GetTargetProcessingSeconds())
	assert.Equal(t, DefaultZeroReplicaSleepSeconds, s.GetZeroReplicaSleepSeconds())
	upcds := uint32(100)
	downcds := uint32(99)
	lbs := uint32(101)
	rpsu := uint32(3)
	rpsd := uint32(4)
	tps := uint32(102)
	tbu := uint32(33)
	zrss := uint32(44)
	s = Scale{
		Min:                      ptr.To[int32](2),
		Max:                      ptr.To[int32](4),
		ScaleUpCooldownSeconds:   &upcds,
		ScaleDownCooldownSeconds: &downcds,
		LookbackSeconds:          &lbs,
		ReplicasPerScaleUp:       &rpsu,
		ReplicasPerScaleDown:     &rpsd,
		TargetProcessingSeconds:  &tps,
		TargetBufferAvailability: &tbu,
		ZeroReplicaSleepSeconds:  &zrss,
	}
	assert.Equal(t, int32(2), s.GetMinReplicas())
	assert.Equal(t, int32(4), s.GetMaxReplicas())
	assert.Equal(t, int(upcds), s.GetScaleUpCooldownSeconds())
	assert.Equal(t, int(downcds), s.GetScaleDownCooldownSeconds())
	assert.Equal(t, int(lbs), s.GetLookbackSeconds())
	assert.Equal(t, int(rpsu), s.GetReplicasPerScaleUp())
	assert.Equal(t, int(rpsd), s.GetReplicasPerScaleDown())
	assert.Equal(t, int(tbu), s.GetTargetBufferAvailability())
	assert.Equal(t, int(tps), s.GetTargetProcessingSeconds())
	assert.Equal(t, int(zrss), s.GetZeroReplicaSleepSeconds())
	s.Max = ptr.To[int32](500)
	assert.Equal(t, int32(500), s.GetMaxReplicas())
}

func TestScale_GetActiveCronSchedule(t *testing.T) {
	s := Scale{
		Cron: &CronScheduling{
			Timezone: "UTC",
			Schedules: []CronSchedule{
				{
					Start: "0 2 * * *",
					End:   "0 3 * * *",
					Min:   ptr.To[int32](1),
					Max:   ptr.To[int32](5),
				},
			},
		},
	}

	schedule, active := s.GetActiveCronSchedule(time.Date(2026, 7, 23, 2, 30, 0, 0, time.UTC))
	assert.True(t, active)
	assert.Equal(t, int32(1), *schedule.Min)
	assert.Equal(t, int32(5), *schedule.Max)

	schedule, active = s.GetActiveCronSchedule(time.Date(2026, 7, 23, 3, 0, 0, 0, time.UTC))
	assert.False(t, active)
	assert.Nil(t, schedule)
}

func TestScale_GetEffectiveScaleBounds(t *testing.T) {
	now := time.Now().UTC()
	start := now.Add(-time.Minute)
	end := now.Add(time.Minute)
	cronExpression := func(t time.Time) string {
		return fmt.Sprintf("%d %d %d %d *", t.Minute(), t.Hour(), t.Day(), int(t.Month()))
	}
	s := Scale{
		Min: ptr.To[int32](0),
		Max: ptr.To[int32](50),
		Cron: &CronScheduling{
			Timezone: "UTC",
			Schedules: []CronSchedule{
				{
					Start: cronExpression(start),
					End:   cronExpression(end),
					Min:   ptr.To[int32](1),
					Max:   ptr.To[int32](5),
				},
			},
		},
	}

	min, max, active := s.GetEffectiveScaleBounds()
	assert.True(t, active)
	assert.Equal(t, int32(1), min)
	assert.Equal(t, int32(5), max)
}
