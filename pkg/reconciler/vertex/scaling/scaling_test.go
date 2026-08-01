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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

var (
	fakeVertex = &dfv1.Vertex{
		Spec: dfv1.VertexSpec{
			Replicas: ptr.To[int32](3),
			AbstractVertex: dfv1.AbstractVertex{
				Scale: dfv1.Scale{
					TargetProcessingSeconds:  ptr.To[uint32](5),
					TargetBufferAvailability: ptr.To[uint32](90),
				},
			},
		},
		Status: dfv1.VertexStatus{
			Replicas:      uint32(3),
			ReadyReplicas: uint32(2),
		},
	}
)

func Test_BasicOperations(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	s := NewScaler(cl)
	assert.NotNil(t, s)
	s.StartWatching("key1")
	assert.True(t, s.Contains("key1"))
	assert.Equal(t, 1, s.Length())
	s.StopWatching("key1")
	assert.False(t, s.Contains("key1"))
}
func Test_desiredReplicasSinglePartition(t *testing.T) {
	t.Run("test src", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		s := NewScaler(cl)
		src := fakeVertex.DeepCopy()
		src.Spec.Source = &dfv1.Source{
			Kafka: &dfv1.KafkaSource{},
		}
		src.Spec.Scale.TargetProcessingSeconds = ptr.To[uint32](5)
		assert.Equal(t, int32(1), s.desiredReplicas(context.TODO(), src, []float64{0, 0}, []int64{0, 0}, int64(30000), int64(24000), int64(27000)))
		assert.Equal(t, int32(16), s.desiredReplicas(context.TODO(), src, []float64{250, 500, 750}, []int64{10010, 800, 1200}, int64(30000), int64(24000), int64(27000)))
		assert.Equal(t, int32(9), s.desiredReplicas(context.TODO(), src, []float64{450, 500, 750}, []int64{10010, 800, 1200}, int64(30000), int64(24000), int64(27000)))
		assert.Equal(t, int32(16), s.desiredReplicas(context.TODO(), src, []float64{450, 20, 750}, []int64{10010, 800, 1200}, int64(30000), int64(24000), int64(27000)))
		assert.Equal(t, int32(17), s.desiredReplicas(context.TODO(), src, []float64{450, 20, 750}, []int64{18932, 800, 1200}, int64(30000), int64(24000), int64(27000)))
		assert.Equal(t, int32(13), s.desiredReplicas(context.TODO(), src, []float64{800, 200, 750}, []int64{18932, 800, 24988}, int64(30000), int64(24000), int64(27000)))
		assert.Equal(t, int32(13), s.desiredReplicas(context.TODO(), src, []float64{800, 210, 750}, []int64{18932, 800, 24988}, int64(30000), int64(24000), int64(27000)))
		assert.Equal(t, int32(15), s.desiredReplicas(context.TODO(), src, []float64{800, 21, 750}, []int64{18932, 800, 24988}, int64(30000), int64(24000), int64(27000)))
	})
	t.Run("test udf", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		s := NewScaler(cl)
		udf := fakeVertex.DeepCopy()
		udf.Spec.UDF = &dfv1.UDF{}
		udf.Spec.Scale.TargetProcessingSeconds = ptr.To[uint32](5)
		udf.Spec.Scale.TargetBufferAvailability = ptr.To[uint32](90)
		tests := []struct {
			name                    string
			partitionProcessingRate []float64
			partitionPending        []int64
			readyReplicas           uint32
			want                    int32
		}{
			{
				name:                    "no data",
				partitionProcessingRate: []float64{0, 0},
				partitionPending:        []int64{0, 0},
				readyReplicas:           2,
				want:                    1,
			},
			{
				name:                    "using targetProcessingSeconds",
				partitionProcessingRate: []float64{250, 500, 750},
				partitionPending:        []int64{2789, 800, 1200},
				readyReplicas:           2,
				want:                    4,
			},
			{
				name:                    "at the edge of switching from targetProcessingSeconds to targetBufferAvailability, still targetProcessingSeconds",
				partitionProcessingRate: []float64{250, 500, 750},
				partitionPending:        []int64{2999, 800, 1200},
				readyReplicas:           2,
				want:                    5,
			},
			{
				name:                    "at the edge of using targetBufferAvailability, just switched from using targetProcessingSeconds",
				partitionProcessingRate: []float64{250, 500, 750},
				partitionPending:        []int64{3001, 800, 1200},
				readyReplicas:           5,
				want:                    6,
			},
			{
				name:                    "combined using targetBufferAvailability and targetProcessingSeconds for differetn partitions",
				partitionProcessingRate: []float64{250, 500, 750},
				partitionPending:        []int64{8001, 800, 1200},
				readyReplicas:           6,
				want:                    10,
			},
			{
				name:                    "using targetBufferAvailability",
				partitionProcessingRate: []float64{250, 500, 750},
				partitionPending:        []int64{8001, 3882, 4002},
				readyReplicas:           9,
				want:                    15,
			},
			{
				name:                    "using targetBufferAvailability, less pending",
				partitionProcessingRate: []float64{250, 500, 750},
				partitionPending:        []int64{6000, 3882, 4002},
				readyReplicas:           9,
				want:                    14,
			},
			{
				name:                    "using targetBufferAvailability, higher rate, no change",
				partitionProcessingRate: []float64{650, 570, 790},
				partitionPending:        []int64{6000, 3882, 4002},
				readyReplicas:           9,
				want:                    14,
			},
			{
				name:                    "using targetBufferAvailability, more replicas, less pending",
				partitionProcessingRate: []float64{650, 570, 790},
				partitionPending:        []int64{3100, 3001, 3223},
				readyReplicas:           11,
				want:                    14,
			},
			{
				name:                    "using targetProcessingSeconds, more replicas, less pending",
				partitionProcessingRate: []float64{650, 570, 790},
				partitionPending:        []int64{2998, 2998, 2998},
				readyReplicas:           13,
				want:                    14,
			},
			{
				name:                    "using targetProcessingSeconds, less and less pending",
				partitionProcessingRate: []float64{650, 570, 790},
				partitionPending:        []int64{2501, 1283, 2044},
				readyReplicas:           14,
				want:                    11,
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				udf := fakeVertex.DeepCopy()
				udf.Spec.UDF = &dfv1.UDF{}
				udf.Spec.Scale.TargetProcessingSeconds = ptr.To[uint32](5)
				udf.Spec.Scale.TargetBufferAvailability = ptr.To[uint32](90)
				udf.Status.ReadyReplicas = tt.readyReplicas
				got := s.desiredReplicas(context.TODO(), udf, tt.partitionProcessingRate, tt.partitionPending, int64(30000), int64(24000), int64(27000))
				assert.Equal(t, tt.want, got)
			})
		}
	})
}

func TestScaleOneVertex_AppliesActiveCronBoundsBeforeMetrics(t *testing.T) {
	tests := []struct {
		name                 string
		current              int32
		cronMin              int32
		cronMax              int32
		replicasPerScaleUp   uint32
		replicasPerScaleDown uint32
		scaleUpCooldown      uint32
		scaleDownCooldown    uint32
		lastScaledSecondsAgo int
		expected             int32
	}{
		{
			// diff=1-0=1, maxAllowedUp=2 → clamped diff=1 → 0+1=1
			name:                 "scale up from zero for nightly DLQ drain",
			current:              0,
			cronMin:              1,
			cronMax:              5,
			replicasPerScaleUp:   2,
			replicasPerScaleDown: 2,
			scaleUpCooldown:      90,
			scaleDownCooldown:    90,
			lastScaledSecondsAgo: 600,
			expected:             1,
		},
		{
			// diff=10-5=5, maxAllowedDown=2 → clamped diff=2 → 10-2=8
			name:                 "scale down toward cron max",
			current:              10,
			cronMin:              1,
			cronMax:              5,
			replicasPerScaleUp:   2,
			replicasPerScaleDown: 2,
			scaleUpCooldown:      90,
			scaleDownCooldown:    90,
			lastScaledSecondsAgo: 600,
			expected:             8,
		},
		{
			// secondsSinceLastScale≈60: global early-exit requires BOTH cooldowns active (60<30 && 60<90)=false → proceeds;
			// scaleUpVertex sees 60 < 90 → cooldown active → no patch.
			name:                 "scale up cooldown prevents scaling",
			current:              0,
			cronMin:              1,
			cronMax:              5,
			replicasPerScaleUp:   2,
			replicasPerScaleDown: 2,
			scaleUpCooldown:      90,
			scaleDownCooldown:    30,
			lastScaledSecondsAgo: 60,
			expected:             0, // no change
		},
		{
			// secondsSinceLastScale≈60: global early-exit (60<90 && 60<30)=false → proceeds;
			// scaleDownVertex sees 60 < 90 → cooldown active → no patch.
			name:                 "scale down cooldown prevents scaling",
			current:              10,
			cronMin:              1,
			cronMax:              5,
			replicasPerScaleUp:   2,
			replicasPerScaleDown: 2,
			scaleUpCooldown:      30,
			scaleDownCooldown:    90,
			lastScaledSecondsAgo: 60,
			expected:             10, // no change
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
				return fmt.Sprintf("0 %d %d %d %d *", t.Minute(), t.Hour(), t.Day(), int(t.Month()))
			}

			pl := &dfv1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pl",
					Namespace: "default",
				},
				Spec: dfv1.PipelineSpec{
					Lifecycle: dfv1.Lifecycle{
						DesiredPhase: dfv1.PipelinePhaseRunning,
					},
				},
			}
			vertex := &dfv1.Vertex{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pl-source",
					Namespace: "default",
				},
				Spec: dfv1.VertexSpec{
					Replicas:     ptr.To(tc.current),
					PipelineName: "test-pl",
					AbstractVertex: dfv1.AbstractVertex{
						Name:   "source",
						Source: &dfv1.Source{},
						Scale: dfv1.Scale{
							ReplicasPerScaleUp:       ptr.To(tc.replicasPerScaleUp),
							ReplicasPerScaleDown:     ptr.To(tc.replicasPerScaleDown),
							ScaleUpCooldownSeconds:   ptr.To(tc.scaleUpCooldown),
							ScaleDownCooldownSeconds: ptr.To(tc.scaleDownCooldown),
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
				},
				Status: dfv1.VertexStatus{
					Phase:           dfv1.VertexPhaseRunning,
					Replicas:        uint32(tc.current),
					DesiredReplicas: uint32(tc.current),
					LastScaledAt:    metav1.NewTime(time.Now().Add(-time.Duration(tc.lastScaledSecondsAgo) * time.Second)),
				},
			}

			scheme := runtime.NewScheme()
			require.NoError(t, dfv1.AddToScheme(scheme))
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pl, vertex).Build()
			scaler := NewScaler(cl)

			err = scaler.scaleOneVertex(context.Background(), "default/test-pl-source", 1)
			assert.NoError(t, err)

			updated := &dfv1.Vertex{}
			assert.NoError(t, cl.Get(context.Background(), client.ObjectKeyFromObject(vertex), updated))
			assert.Equal(t, tc.expected, *updated.Spec.Replicas)
		})
	}
}
