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
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
)

func Test_Sink_getContainers(t *testing.T) {
	s := Sink{}
	c, err := s.getContainers(getContainerReq{
		env: []corev1.EnvVar{
			{Name: "test-env", Value: "test-val"},
		},
		isbSvcType:      ISBSvcTypeJetStream,
		imagePullPolicy: corev1.PullIfNotPresent,
		image:           testFlowImage,
		resources:       corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(c))
	assert.Equal(t, testFlowImage, c[0].Image)
	assert.Equal(t, corev1.ResourceRequirements{Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("2")}}, c[0].Resources)
}

func Test_Sink_getUDSinkContainer(t *testing.T) {
	x := Sink{
		AbstractSink: AbstractSink{
			UDSink: &UDSink{
				Container: &Container{
					Image:           "my-image",
					Args:            []string{"my-arg"},
					SecurityContext: &corev1.SecurityContext{},
					EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "test-cm"},
					}}},
				},
			},
		},
	}
	c := x.getUDSinkContainer(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, CtrUdsink, c.Name)
	assert.NotNil(t, c.SecurityContext)
	assert.Equal(t, corev1.PullAlways, c.ImagePullPolicy)
	assert.Equal(t, "my-image", c.Image)
	assert.Contains(t, c.Args, "my-arg")
	assert.Equal(t, 1, len(c.EnvFrom))
	envs := map[string]string{}
	for _, e := range c.Env {
		envs[e.Name] = e.Value
	}
	assert.Equal(t, envs[EnvUDContainerType], UDContainerSink)
	x.UDSink.Container.ImagePullPolicy = &testImagePullPolicy
	c = x.getUDSinkContainer(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, testImagePullPolicy, c.ImagePullPolicy)
	assert.True(t, c.LivenessProbe != nil)
}

func Test_Sink_getFallbackUDSinkContainer(t *testing.T) {
	x := Sink{
		AbstractSink: AbstractSink{
			UDSink: &UDSink{
				Container: &Container{
					Image:           "my-image",
					Args:            []string{"my-arg"},
					SecurityContext: &corev1.SecurityContext{},
					EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "test-cm"},
					}}},
				},
			},
		},
		Fallback: &AbstractSink{
			UDSink: &UDSink{
				Container: &Container{
					Image:           "my-image",
					Args:            []string{"my-arg"},
					SecurityContext: &corev1.SecurityContext{},
					EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "test-cm"},
					}}},
				},
			},
		},
	}
	c := x.getFallbackUDSinkContainer(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, CtrFallbackUdsink, c.Name)
	assert.NotNil(t, c.SecurityContext)
	assert.Equal(t, corev1.PullAlways, c.ImagePullPolicy)
	assert.Equal(t, "my-image", c.Image)
	assert.Contains(t, c.Args, "my-arg")
	assert.Equal(t, 1, len(c.EnvFrom))
	envs := map[string]string{}
	for _, e := range c.Env {
		envs[e.Name] = e.Value
	}
	assert.Equal(t, envs[EnvUDContainerType], UDContainerFallbackSink)
	x.UDSink.Container.ImagePullPolicy = &testImagePullPolicy
	c = x.getUDSinkContainer(getContainerReq{
		image:           "main-image",
		imagePullPolicy: corev1.PullAlways,
	})
	assert.Equal(t, testImagePullPolicy, c.ImagePullPolicy)
	assert.True(t, c.LivenessProbe != nil)
}

func TestIsValidSinkRetryStrategy(t *testing.T) {
	tests := []struct {
		name     string
		sink     *Sink
		strategy RetryStrategy
		wantErr  bool
	}{
		{
			name: "valid strategy with fallback configured",
			sink: &Sink{Fallback: &AbstractSink{
				UDSink: &UDSink{},
			}},
			strategy: RetryStrategy{
				OnFailure: func() *OnFailureRetryStrategy { str := OnFailureFallback; return &str }(),
			},
			wantErr: false,
		},
		{
			name: "invalid valid strategy with fallback not configured properly",
			sink: &Sink{Fallback: &AbstractSink{}},
			strategy: RetryStrategy{
				OnFailure: func() *OnFailureRetryStrategy { str := OnFailureFallback; return &str }(),
			},
			wantErr: true,
		},
		{
			name: "invalid strategy with no fallback configured",
			sink: &Sink{},
			strategy: RetryStrategy{
				OnFailure: func() *OnFailureRetryStrategy { str := OnFailureFallback; return &str }(),
			},
			wantErr: true,
		},
		{
			name: "valid strategy with drop and no fallback needed",
			sink: &Sink{},
			strategy: RetryStrategy{
				OnFailure: func() *OnFailureRetryStrategy { str := OnFailureDrop; return &str }(),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.sink.RetryStrategy = tt.strategy
			err := tt.sink.HasValidSinkRetryStrategy()
			if (err != nil) != tt.wantErr {
				t.Errorf("isValidSinkRetryStrategy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

//func TestGetRetryStrategy(t *testing.T) {
//	defaultStrategy := GetDefaultSinkRetryStrategy()
//	customInterval := metav1.Duration{Duration: 2 * time.Second}
//	customSteps := uint32(5)
//
//	tests := []struct {
//		name           string
//		customStrategy *RetryStrategy
//		want           *RetryStrategy
//	}{
//		{
//			name:           "No custom strategy, use defaults",
//			customStrategy: nil,
//			want:           defaultStrategy,
//		},
//		{
//			name: "Custom interval only",
//			customStrategy: &RetryStrategy{
//				BackOff: &Backoff{
//					Interval: &customInterval,
//				},
//			},
//			want: &RetryStrategy{
//				BackOff: &Backoff{
//					Interval: &customInterval,
//					Steps:    defaultStrategy.BackOff.Steps,
//				},
//				OnFailure: defaultStrategy.OnFailure,
//			},
//		},
//		{
//			name: "Custom steps only",
//			customStrategy: &RetryStrategy{
//				BackOff: &Backoff{
//					Steps: &customSteps,
//				},
//			},
//			want: &RetryStrategy{
//				BackOff: &Backoff{
//					Interval: defaultStrategy.BackOff.Interval,
//					Steps:    &customSteps,
//				},
//				OnFailure: defaultStrategy.OnFailure,
//			},
//		},
//		{
//			name: "Full custom strategy",
//			customStrategy: &RetryStrategy{
//				BackOff: &Backoff{
//					Interval: &customInterval,
//					Steps:    &customSteps,
//				},
//				OnFailure: func() *OnFailureRetryStrategy { s := OnFailureDrop; return &s }(),
//			},
//			want: &RetryStrategy{
//				BackOff: &Backoff{
//					Interval: &customInterval,
//					Steps:    &customSteps,
//				},
//				OnFailure: func() *OnFailureRetryStrategy { s := OnFailureDrop; return &s }(),
//			},
//		},
//	}
//
//	s := &Sink{}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			// Setup custom strategy if applicable
//			s.RetryStrategy = tt.customStrategy
//
//			// Get the retry strategy
//			got, _ := s.GetRetryStrategy()
//
//			// Compare results
//			if got.BackOff.Interval != nil && *got.BackOff.Interval != *tt.want.BackOff.Interval {
//				t.Errorf("GetRetryStrategy() got Interval = %v, want %v", *got.BackOff.Interval, *tt.want.BackOff.Interval)
//			}
//			if got.BackOff.Steps != nil && *got.BackOff.Steps != *tt.want.BackOff.Steps {
//				t.Errorf("GetRetryStrategy() got Steps = %v, want %v", *got.BackOff.Steps, *tt.want.BackOff.Steps)
//			}
//			if got.OnFailure != nil && *got.OnFailure != *tt.want.OnFailure {
//				t.Errorf("GetRetryStrategy() got OnFailure = %v, want %v", *got.OnFailure, *tt.want.OnFailure)
//			}
//		})
//	}
//}
