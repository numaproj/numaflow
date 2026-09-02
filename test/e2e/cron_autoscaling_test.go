//go:build test

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

package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type CronAutoscalingSuite struct {
	E2ESuite
}

// cronShortWindowFromNow returns a six-field (second minute hour dom month
// dow) start/end cron expression pair for a window that opens roughly
// startInSeconds from now and stays open for durationSeconds, anchored to the
// current wall clock.
func cronShortWindowFromNow(startInSeconds, durationSeconds int) (start, end string) {
	now := time.Now().UTC()
	from := now.Add(time.Duration(startInSeconds) * time.Second)
	to := from.Add(time.Duration(durationSeconds) * time.Second)
	start = fmt.Sprintf("%d %d %d * * *", from.Second(), from.Minute(), from.Hour())
	end = fmt.Sprintf("%d %d %d * * *", to.Second(), to.Minute(), to.Hour())
	return start, end
}

// TestSourceVertexCronScaleUpFromZero verifies that a Pipeline source vertex
// with an active cron window scales up to the window's min replicas shortly
// after creation, even without any traffic/pending-message metrics being
// available.
func (s *CronAutoscalingSuite) TestSourceVertexCronScaleUpFromZero() {
	start, end := cronShortWindowFromNow(2, 60)
	pl := &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cron-scale-up-pipeline",
		},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name: "input",
					Source: &dfv1.Source{
						Generator: &dfv1.GeneratorSource{
							RPU: ptr.To(int64(5)),
						},
					},
					Scale: dfv1.Scale{
						Min:                      ptr.To[int32](0),
						Max:                      ptr.To[int32](5),
						ScaleUpCooldownSeconds:   ptr.To[uint32](0),
						ScaleDownCooldownSeconds: ptr.To[uint32](0),
						Cron: &dfv1.CronScheduling{
							Timezone: "UTC",
							Schedules: []dfv1.CronSchedule{
								{
									Start: start,
									End:   end,
									Min:   ptr.To[int32](3),
									Max:   ptr.To[int32](5),
								},
							},
						},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{
						AbstractSink: dfv1.AbstractSink{
							Log: &dfv1.Log{},
						},
					},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "output"},
			},
		},
	}

	w := s.Given().WithPipeline(pl).When().CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// The autoscaler should detect the active cron window and scale the
	// source vertex up to at least the window's min (3).
	w.Expect().VertexSizeScaledTo("input", 3)
}

// TestSourceVertexCronScaleDownAfterWindowExpires verifies that once an
// active cron window closes, the autoscaler stops honoring the window's
// bounds and reverts to base scale.min/scale.max, scaling a source vertex
// back down even though it had scaled up while the window was active.
func (s *CronAutoscalingSuite) TestSourceVertexCronScaleDownAfterWindowExpires() {
	// Window opens almost immediately and stays open long enough for the
	// autoscaler to reliably detect it and scale up to 3 before it closes on
	// its own while the test is still running.
	start, end := cronShortWindowFromNow(2, 60)
	pl := &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cron-scale-down-pipeline",
		},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name: "input",
					Source: &dfv1.Source{
						Generator: &dfv1.GeneratorSource{
							RPU: ptr.To(int64(5)),
						},
					},
					// Base bounds cap at 1 so pipeline creation's readiness
					// check (which only knows about base min/max) is
					// satisfied immediately, before the cron window opens.
					Scale: dfv1.Scale{
						Min:                      ptr.To[int32](0),
						Max:                      ptr.To[int32](1),
						ScaleUpCooldownSeconds:   ptr.To[uint32](0),
						ScaleDownCooldownSeconds: ptr.To[uint32](0),
						Cron: &dfv1.CronScheduling{
							Timezone: "UTC",
							Schedules: []dfv1.CronSchedule{
								{
									Start: start,
									End:   end,
									Min:   ptr.To[int32](3),
									Max:   ptr.To[int32](3),
								},
							},
						},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{
						AbstractSink: dfv1.AbstractSink{
							Log: &dfv1.Log{},
						},
					},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "output"},
			},
		},
	}

	w := s.Given().WithPipeline(pl).When().CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// Cron window opens shortly after creation; expect scale-up to 3.
	w.Expect().VertexSizeScaledTo("input", 3)

	// Window has now closed (it only lasted 60s); expect the autoscaler to
	// revert to base bounds and scale back down to max=1.
	w.Expect().VertexSizeScaledTo("input", 1)
}

// TestCronOnNonSourceVertexRejected verifies that configuring scale.cron on a
// non-source (sink) Pipeline vertex is rejected by the validating webhook,
// end to end, rather than silently ignored.
func (s *CronAutoscalingSuite) TestCronOnNonSourceVertexRejected() {
	start, end := cronShortWindowFromNow(2, 60)
	pl := &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name: "cron-non-source-rejected-pipeline",
		},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name: "input",
					Source: &dfv1.Source{
						Generator: &dfv1.GeneratorSource{
							RPU: ptr.To(int64(5)),
						},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{
						AbstractSink: dfv1.AbstractSink{
							Log: &dfv1.Log{},
						},
					},
					// Cron on a non-source (sink) vertex must be rejected.
					Scale: dfv1.Scale{
						Cron: &dfv1.CronScheduling{
							Timezone: "UTC",
							Schedules: []dfv1.CronSchedule{
								{
									Start: start,
									End:   end,
									Min:   ptr.To[int32](3),
									Max:   ptr.To[int32](5),
								},
							},
						},
					},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "output"},
			},
		},
	}

	err := s.Given().CreatePipelineExpectingError(pl)
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "cron autoscaling is only supported for source vertices")
}

func TestCronAutoscalingSuite(t *testing.T) {
	suite.Run(t, new(CronAutoscalingSuite))
}
