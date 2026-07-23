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
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func monoVtxWithScale(targetSec uint32, readyReplicas uint32, currentReplicas uint32) *dfv1.MonoVertex {
	mv := &dfv1.MonoVertex{}
	mv.Spec.Scale.TargetProcessingSeconds = &targetSec
	mv.Status.ReadyReplicas = readyReplicas
	mv.Status.Replicas = currentReplicas
	return mv
}

func TestDesiredReplicas(t *testing.T) {
	s := &Scaler{}
	ctx := context.Background()

	tests := []struct {
		name            string
		pending         int64
		processingRate  float64
		targetSec       uint32
		readyReplicas   uint32
		currentReplicas uint32
		expected        int32
	}{
		{
			name:           "bothZero_scaleToZero",
			pending:        0,
			processingRate: 0,
			targetSec:      20,
			readyReplicas:  1,
			expected:       0,
		},
		{
			name:            "rateZero_returnsCurrent",
			pending:         100,
			processingRate:  0,
			targetSec:       20,
			readyReplicas:   1,
			currentReplicas: 3,
			expected:        3,
		},
		{
			name:           "normal",
			pending:        100,
			processingRate: 5,
			targetSec:      20,
			readyReplicas:  1,
			expected:       1,
		},
		{
			name:           "desiredZero_clampedToOne",
			pending:        1,
			processingRate: 1000,
			targetSec:      20,
			readyReplicas:  1,
			expected:       1,
		},
		{
			// desired = round((3/0.5)/20 * 1) = round(0.3) = 0 → clamped to 1.
			// pending cap (3) > 1 so no further cap applied.
			name:           "capByPending_desiredLessThanPending",
			pending:        3,
			processingRate: 0.5,
			targetSec:      20,
			readyReplicas:  1,
			expected:       1,
		},
		{
			// pending cap path: desired > pending, so cap to pending.
			name:           "capByPending_desiredGreaterThanPending",
			pending:        3,
			processingRate: 0.01,
			targetSec:      1,
			readyReplicas:  5,
			expected:       3,
		},
		{
			// Regression test for issue #3415: pending=100,000, rate=0.001 msg/s, targetSec=20,
			// readyReplicas=1 → raw float64 = 5,000,000,000 which overflows int32 without the fix.
			// After the MaxInt32 float clamp, desired is then capped to pending (100,000) since
			// we must never scale to more replicas than there are messages.
			name:           "overflow_fromIssue3415",
			pending:        100_000,
			processingRate: 0.001,
			targetSec:      20,
			readyReplicas:  1,
			expected:       100_000,
		},
		{
			name:           "extremeOverflow",
			pending:        1_000_000,
			processingRate: 0.0001,
			targetSec:      1,
			readyReplicas:  10,
			expected:       1_000_000,
		},
		{
			// pending > math.MaxInt32: the pending-cap guard must not wrap to negative.
			name:           "pendingExceedsMaxInt32",
			pending:        int64(math.MaxInt32) + 1000,
			processingRate: 1e9,
			targetSec:      20,
			readyReplicas:  1,
			expected:       1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mv := monoVtxWithScale(tc.targetSec, tc.readyReplicas, tc.currentReplicas)
			got := s.desiredReplicas(ctx, mv, tc.processingRate, tc.pending)
			assert.Equal(t, tc.expected, got)
			// Invariant: result must never be negative (except the explicit scale-to-zero case).
			if tc.expected != 0 {
				assert.True(t, got > 0, "desiredReplicas must not return a non-positive value for non-zero expected")
			}
		})
	}
}

func TestScaleOneMonoVertex_AppliesActiveCronBoundsBeforeMetrics(t *testing.T) {
	tests := []struct {
		name                 string
		current              int32
		cronMin              int32
		cronMax              int32
		replicasPerScaleUp   uint32
		replicasPerScaleDown uint32
		expected             int32
	}{
		{
			name:                 "scale up from zero for nightly DLQ drain",
			current:              0,
			cronMin:              1,
			cronMax:              5,
			replicasPerScaleUp:   2,
			replicasPerScaleDown: 2,
			expected:             1,
		},
		{
			name:                 "scale down toward cron max",
			current:              10,
			cronMin:              1,
			cronMax:              5,
			replicasPerScaleUp:   2,
			replicasPerScaleDown: 2,
			expected:             8,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			location, err := time.LoadLocation("America/Los_Angeles")
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().In(location)
			start := now.Add(-time.Minute)
			end := now.Add(time.Minute)
			cronExpression := func(t time.Time) string {
				return fmt.Sprintf("%d %d %d %d *", t.Minute(), t.Hour(), t.Day(), int(t.Month()))
			}
			mv := &dfv1.MonoVertex{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-mvtx",
					Namespace: "default",
				},
				Spec: dfv1.MonoVertexSpec{
					Replicas: ptr.To(tc.current),
					Scale: dfv1.Scale{
						Min:                  ptr.To[int32](0),
						Max:                  ptr.To[int32](50),
						ReplicasPerScaleUp:   ptr.To(tc.replicasPerScaleUp),
						ReplicasPerScaleDown: ptr.To(tc.replicasPerScaleDown),
						Cron: &dfv1.CronScheduling{
							Timezone: "America/Los_Angeles",
							Schedules: []dfv1.CronSchedule{
								{
									Start: cronExpression(start),
									End:   cronExpression(end),
									Min:   ptr.To(tc.cronMin),
									Max:   ptr.To(tc.cronMax),
								},
							},
						},
					},
				},
				Status: dfv1.MonoVertexStatus{
					Phase:        dfv1.MonoVertexPhaseRunning,
					Replicas:     uint32(tc.current),
					LastScaledAt: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
				},
			}
			scheme := runtime.NewScheme()
			assert.NoError(t, dfv1.AddToScheme(scheme))
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(mv).Build()
			scaler := NewScaler(cl)

			err = scaler.scaleOneMonoVertex(context.Background(), "default/test-mvtx", 1)
			assert.NoError(t, err)

			updated := &dfv1.MonoVertex{}
			assert.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(mv), updated))
			assert.Equal(t, tc.expected, *updated.Spec.Replicas)
		})
	}
}
