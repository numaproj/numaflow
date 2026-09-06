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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func TestParseCronSchedules(t *testing.T) {
	t.Run("valid schedule", func(t *testing.T) {
		cs := &dfv1.CronScheduling{
			Schedules: []dfv1.CronSchedule{
				{
					Start: "0 0 9 * * *",
					End:   "0 0 17 * * *",
				},
			},
		}

		parsed, err := ParseCronSchedules(cs)
		assert.NoError(t, err)
		assert.Len(t, parsed, 1)
	})

	t.Run("invalid start", func(t *testing.T) {
		cs := &dfv1.CronScheduling{
			Schedules: []dfv1.CronSchedule{
				{
					Start: "invalid",
					End:   "0 0 17 * * *",
				},
			},
		}

		_, err := ParseCronSchedules(cs)
		assert.Error(t, err)
	})

	t.Run("invalid end", func(t *testing.T) {
		cs := &dfv1.CronScheduling{
			Schedules: []dfv1.CronSchedule{
				{
					Start: "0 0 9 * * *",
					End:   "invalid",
				},
			},
		}

		_, err := ParseCronSchedules(cs)
		assert.Error(t, err)
	})
}

func TestEffectiveScaleBoundsAt(t *testing.T) {
	min := int32(5)
	max := int32(10)

	scale := dfv1.Scale{
		Min: ptr.To[int32](1),
		Max: ptr.To[int32](3),
		Cron: &dfv1.CronScheduling{
			Timezone: "UTC",
			Schedules: []dfv1.CronSchedule{
				{
					Start: "0 0 9 * * *",
					End:   "0 0 17 * * *",
					Min:   &min,
					Max:   &max,
				},
			},
		},
	}

	parsed, err := ParseCronSchedules(scale.Cron)
	assert.NoError(t, err)

	t.Run("active", func(t *testing.T) {
		now := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)

		gotMin, gotMax, active := EffectiveScaleBoundsAt(scale, parsed, now)

		assert.True(t, active)
		assert.Equal(t, min, gotMin)
		assert.Equal(t, max, gotMax)
	})

	t.Run("inactive", func(t *testing.T) {
		now := time.Date(2026, 1, 5, 20, 0, 0, 0, time.UTC)

		gotMin, gotMax, active := EffectiveScaleBoundsAt(scale, parsed, now)

		assert.False(t, active)
		assert.Equal(t, int32(1), gotMin)
		assert.Equal(t, int32(3), gotMax)
	})
}
