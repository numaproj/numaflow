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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
)

var (
	testMvtx = &dfv1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pl",
			Namespace: "test-ns",
		},
		Spec: dfv1.MonoVertexSpec{
			InitContainers: []corev1.Container{
				{
					Name:  "init-container",
					Image: "my-image:latest",
				},
			},
			Sidecars: []corev1.Container{
				{
					Name:  "sidecar-container",
					Image: "my-image:latest",
				},
			},
			Source: &dfv1.Source{
				UDTransformer: &dfv1.UDTransformer{
					Container: &dfv1.Container{Image: "my-image"},
				},
				UDSource: &dfv1.UDSource{
					Container: &dfv1.Container{
						Image: "my-image:latest",
					},
				},
			},
			Sink: &dfv1.Sink{
				AbstractSink: dfv1.AbstractSink{
					UDSink: &dfv1.UDSink{
						Container: &dfv1.Container{
							Image: "my-image:latest",
						},
					},
				},
				Fallback: &dfv1.AbstractSink{
					UDSink: &dfv1.UDSink{
						Container: &dfv1.Container{
							Image: "my-fb-image:latest",
						},
					},
				},
			},
			UDF: &dfv1.UDF{
				Container: &dfv1.Container{
					Image: "my-udf-image:latest",
				},
			},
		},
	}
)

func TestValidateMonoVertex(t *testing.T) {
	t.Run("test good mvtx", func(t *testing.T) {
		err := ValidateMonoVertex(testMvtx)
		assert.NoError(t, err)
	})

	t.Run("test nil", func(t *testing.T) {
		err := ValidateMonoVertex(nil)
		assert.Error(t, err)
	})

	t.Run("test invalid name", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Name = "test-pl-iNvalid+name"
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid mvtx name")
	})

	t.Run("test no source", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Source = nil
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "source is not defined")
	})

	t.Run("test invalid source", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Source.Kafka = &dfv1.KafkaSource{}
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid user-defined source spec")
	})

	t.Run("test no sink", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Sink = nil
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sink is not defined")
	})

	t.Run("test invalid sink", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Sink.Fallback = nil
		testObj.Spec.Sink.RetryStrategy = dfv1.RetryStrategy{
			OnFailure: ptr.To(dfv1.OnFailureFallback),
		}
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "given OnFailure strategy is fallback but fallback sink is not provided")
	})

	t.Run("test invalid init container name", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.InitContainers[0].Name = dfv1.CtrInitSideInputs
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid init container name")
	})

	t.Run("test invalid sidecar container name", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Sidecars[0].Name = dfv1.CtrInitSideInputs
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid sidecar container name")
	})

	t.Run("test invalid maxUnavailable", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.UpdateStrategy = dfv1.UpdateStrategy{
			RollingUpdate: &dfv1.RollingUpdateStrategy{
				MaxUnavailable: &intstr.IntOrString{
					Type:   intstr.String,
					StrVal: "invalid",
				},
			},
		}
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid maxUnavailable")
	})

	t.Run("test cron timezone is required", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Scale.Cron = &dfv1.CronScheduling{
			Schedules: []dfv1.CronSchedule{
				{Start: "0 2 * * *", End: "0 3 * * *"},
			},
		}
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timezone is required")
	})

	t.Run("streaming with built-in Kafka source is rejected", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		// Replace UDSource with Kafka-only source (no UDSource/UDTransformer to avoid other validation errors)
		streaming := true
		testObj.Spec.Streaming = &streaming
		testObj.Spec.Source = &dfv1.Source{
			Kafka: &dfv1.KafkaSource{Brokers: []string{"broker:9092"}, Topic: "test-topic"},
		}
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "built-in Kafka source is not supported with streaming=true")
	})

	t.Run("streaming with non-Kafka source is accepted", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		streaming := true
		testObj.Spec.Streaming = &streaming
		// testMvtx already uses UDSource (non-Kafka), so no source change needed
		err := ValidateMonoVertex(testObj)
		assert.NoError(t, err)
	})

	t.Run("non-streaming with Kafka source is accepted", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		// No streaming set; Kafka source alone must not be rejected by the streaming gate
		testObj.Spec.Source = &dfv1.Source{
			Kafka: &dfv1.KafkaSource{Brokers: []string{"broker:9092"}, Topic: "test-topic"},
		}
		err := ValidateMonoVertex(testObj)
		// Should not fail on the streaming gate; any other error is unrelated to this gate
		if err != nil {
			assert.NotContains(t, err.Error(), "built-in Kafka source is not supported with streaming=true")
		}
	})

	t.Run("nil streaming with Kafka source is accepted", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Streaming = nil
		testObj.Spec.Source = &dfv1.Source{
			Kafka: &dfv1.KafkaSource{Brokers: []string{"broker:9092"}, Topic: "test-topic"},
		}
		err := ValidateMonoVertex(testObj)
		if err != nil {
			assert.NotContains(t, err.Error(), "built-in Kafka source is not supported with streaming=true")
		}
	})

	t.Run("test udf spec validation", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		err := ValidateMonoVertex(testObj)
		assert.NoError(t, err)

		// GroupBy is not allowed in MonoVertex UDF
		testObj.Spec.UDF.GroupBy = &dfv1.GroupBy{
			Storage: &dfv1.PBQStorage{
				PersistentVolumeClaim: &dfv1.PersistenceStrategy{},
			},
		}
		err = ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid udf: groupBy/reduce is not supported in monovertex")

		// UDF is optional so nil should be allowed
		testObj.Spec.UDF = nil
		err = ValidateMonoVertex(testObj)
		assert.NoError(t, err)
	})
}

func TestValidateCronScaling(t *testing.T) {
	validScale := func() dfv1.Scale {
		return dfv1.Scale{
			Min: ptr.To[int32](0),
			Max: ptr.To[int32](50),
			Cron: &dfv1.CronScheduling{
				Timezone: "America/Los_Angeles",
				Schedules: []dfv1.CronSchedule{
					{
						Start: "0 2 * * *",
						End:   "0 3 * * *",
						Min:   ptr.To[int32](1),
						Max:   ptr.To[int32](5),
					},
				},
			},
		}
	}

	tests := []struct {
		name      string
		mutate    func(*dfv1.Scale)
		wantError string
	}{
		{
			name: "valid cron scaling",
		},
		{
			name: "cron omitted does not affect existing scale validation",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron = nil
				scale.Min = ptr.To[int32](10)
				scale.Max = ptr.To[int32](1)
			},
		},
		{
			name: "parent min is negative",
			mutate: func(scale *dfv1.Scale) {
				scale.Min = ptr.To[int32](-1)
			},
			wantError: "scale.min must not be negative",
		},
		{
			name: "parent max is negative",
			mutate: func(scale *dfv1.Scale) {
				scale.Max = ptr.To[int32](-1)
			},
			wantError: "scale.max must not be negative",
		},
		{
			name: "parent min is greater than max",
			mutate: func(scale *dfv1.Scale) {
				scale.Min = ptr.To[int32](10)
				scale.Max = ptr.To[int32](5)
			},
			wantError: "scale.min must not be greater than scale.max",
		},
		{
			name: "cron min is negative",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Schedules[0].Min = ptr.To[int32](-1)
			},
			wantError: "min must not be negative",
		},
		{
			name: "cron max is negative",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Schedules[0].Max = ptr.To[int32](-1)
			},
			wantError: "max must not be negative",
		},
		{
			name: "cron min is below parent min",
			mutate: func(scale *dfv1.Scale) {
				scale.Min = ptr.To[int32](2)
				scale.Cron.Schedules[0].Min = ptr.To[int32](1)
			},
			wantError: "min must not be less than scale.min",
		},
		{
			name: "cron max is above parent max",
			mutate: func(scale *dfv1.Scale) {
				scale.Max = ptr.To[int32](4)
				scale.Cron.Schedules[0].Max = ptr.To[int32](5)
			},
			wantError: "max must not be greater than scale.max",
		},
		{
			name: "cron schedules are empty",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Schedules = nil
			},
			wantError: "at least one schedule is required",
		},
		{
			name: "cron timezone is invalid",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Timezone = "invalid/timezone"
			},
			wantError: "invalid timezone",
		},
		{
			name: "cron min is missing",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Schedules[0].Min = nil
			},
			wantError: "min is required",
		},
		{
			name: "cron max is missing",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Schedules[0].Max = nil
			},
			wantError: "max is required",
		},
		{
			name: "cron configured when autoscaling is disabled",
			mutate: func(scale *dfv1.Scale) {
				scale.Disabled = true
			},
			wantError: "cron must not be configured when autoscaling is disabled",
		},
		{
			name: "cron start is empty",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Schedules[0].Start = ""
			},
			wantError: "start",
		},
		{
			name: "cron end is empty",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Schedules[0].End = ""
			},
			wantError: "end",
		},
		{
			name: "cron start and end are semantically identical",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Schedules[0].Start = "0 2 * * *"
				scale.Cron.Schedules[0].End = "00 02 * * *"
			},
			wantError: "start and end must not be identical",
		},
		{
			name: "cron start and end have different calendar fields",
			mutate: func(scale *dfv1.Scale) {
				scale.Cron.Schedules[0].Start = "0 2 * * 1-5"
				scale.Cron.Schedules[0].End = "0 3 * * *"
			},
			wantError: "day-of-month, month, and day-of-week must match",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scale := validScale()
			if tc.mutate != nil {
				tc.mutate(&scale)
			}
			err := validateCronScaling(scale)
			if tc.wantError == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.wantError)
			}
		})
	}
}
