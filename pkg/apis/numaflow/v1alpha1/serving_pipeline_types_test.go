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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
)

func Test_GenerateSourceStreamName(t *testing.T) {
	sp := ServingPipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-serving-pipeline",
		},
	}
	assert.Equal(t, "serving-source-test-serving-pipeline", sp.GenerateSourceStreamName())
}

func Test_GetServingServiceName(t *testing.T) {
	sp := ServingPipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-serving-pipeline",
		},
	}
	assert.Equal(t, "test-serving-pipeline-serving", sp.GetServingServiceName())
}

func Test_ServingPipelineStatus_SetPhase(t *testing.T) {
	spls := ServingPipelineStatus{}
	spls.SetPhase(ServingPipelinePhaseRunning, "Running phase")
	assert.Equal(t, ServingPipelinePhaseRunning, spls.Phase)
	assert.Equal(t, "Running phase", spls.Message)
}

func Test_ServingPipelineStatus_InitConditions(t *testing.T) {
	spls := ServingPipelineStatus{}
	spls.InitConditions()
	assert.Equal(t, 2, len(spls.Conditions))
	for _, c := range spls.Conditions {
		assert.Equal(t, metav1.ConditionUnknown, c.Status)
	}
}

func Test_ServingPipelineStatus_MarkConfigured(t *testing.T) {
	spls := ServingPipelineStatus{}
	spls.InitConditions()
	spls.MarkConfigured()
	for _, c := range spls.Conditions {
		if c.Type == string(ServingPipelineConditionConfigured) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
		}
	}
}

func Test_ServingPipelineStatus_MarkNotConfigured(t *testing.T) {
	spls := ServingPipelineStatus{}
	spls.InitConditions()
	spls.MarkNotConfigured("reason", "message")
	for _, c := range spls.Conditions {
		if c.Type == string(ServingPipelineConditionConfigured) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
		}
	}
	assert.Equal(t, ServingPipelinePhaseFailed, spls.Phase)
}

func Test_ServingPipelineStatus_MarkDeployed(t *testing.T) {
	spls := ServingPipelineStatus{}
	spls.InitConditions()
	spls.MarkDeployed()
	for _, c := range spls.Conditions {
		if c.Type == string(ServingPipelineConditionDeployed) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
		}
	}
}

func Test_ServingPipelineStatus_MarkDeployFailed(t *testing.T) {
	spls := ServingPipelineStatus{}
	spls.InitConditions()
	spls.MarkDeployFailed("reason", "message")
	for _, c := range spls.Conditions {
		if c.Type == string(ServingPipelineConditionDeployed) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
		}
	}
	assert.Equal(t, ServingPipelinePhaseFailed, spls.Phase)
}

func Test_ServingPipelineStatus_IsHealthy(t *testing.T) {
	tests := []struct {
		name  string
		phase ServingPipelinePhase
		ready bool
		want  bool
	}{
		{
			name:  "Failed phase",
			phase: ServingPipelinePhaseFailed,
			ready: false,
			want:  false,
		},
		{
			name:  "Running phase and ready",
			phase: ServingPipelinePhaseRunning,
			ready: true,
			want:  true,
		},
		{
			name:  "Running phase and not ready",
			phase: ServingPipelinePhaseRunning,
			ready: false,
			want:  false,
		},
		{
			name:  "Deleting phase",
			phase: ServingPipelinePhaseDeleting,
			ready: false,
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spls := &ServingPipelineStatus{
				Phase: tt.phase,
			}
			if tt.ready {
				spls.Conditions = []metav1.Condition{
					{
						Type:   string(ServingPipelineConditionConfigured),
						Status: metav1.ConditionTrue,
					},
					{
						Type:   string(ServingPipelineConditionDeployed),
						Status: metav1.ConditionTrue,
					},
				}
			}
			got := spls.IsHealthy()
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_getStoreSidecarContainerSpec(t *testing.T) {
	sp := ServingPipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-serving-pipeline",
			Namespace: "test-namespace",
		},
		Spec: ServingPipelineSpec{
			Serving: ServingSpec{
				ServingStore: &ServingStore{
					Container: &Container{
						Image: "test-image",
						Env: []corev1.EnvVar{
							{Name: "TEST_ENV", Value: "test-value"},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "test-volume", MountPath: "/test-path"},
						},
						Resources: corev1.ResourceRequirements{},
					},
				},
			},
		},
	}

	containerReq := getContainerReq{
		image:           "default-image",
		imagePullPolicy: corev1.PullIfNotPresent,
		resources:       corev1.ResourceRequirements{},
	}

	containers := sp.getStoreSidecarContainerSpec(containerReq)
	assert.Equal(t, 1, len(containers))
	container := containers[0]

	assert.Equal(t, "test-image", container.Image)
	assert.Equal(t, corev1.PullIfNotPresent, container.ImagePullPolicy)
	assert.Equal(t, "test-volume", container.VolumeMounts[0].Name)
	assert.Equal(t, "/test-path", container.VolumeMounts[0].MountPath)
	assert.Equal(t, "TEST_ENV", container.Env[0].Name)
	assert.Equal(t, "test-value", container.Env[0].Value)
	assert.NotNil(t, container.LivenessProbe)
	assert.Equal(t, "/sidecar-livez", container.LivenessProbe.HTTPGet.Path)
	assert.Equal(t, int32(VertexMetricsPort), container.LivenessProbe.HTTPGet.Port.IntVal)
}

func Test_GetPipelineObj(t *testing.T) {
	sp := ServingPipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-serving-pipeline",
			Namespace: "test-namespace",
		},
		Spec: ServingPipelineSpec{
			Pipeline: PipelineSpec{
				Vertices: []AbstractVertex{
					{Name: "input", Source: &Source{}},
					{Name: "output", Sink: &Sink{}},
				},
				Edges: []Edge{
					{From: "input", To: "output"},
				},
			},
			Serving: ServingSpec{
				Service: true,
				ContainerTemplate: &ContainerTemplate{
					Env: []corev1.EnvVar{{Name: "TEST_ENV", Value: "test-value"}},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("1942Mi"),
						},
					},
				},
			},
		},
	}

	req := GetServingPipelineResourceReq{
		ISBSvcConfig: BufferServiceConfig{
			JetStream: &JetStreamConfig{
				URL: "nats://test-url",
			},
		},
		Image:      "test-image",
		PullPolicy: corev1.PullIfNotPresent,
	}

	pipeline := sp.GetPipelineObj(req)

	assert.Equal(t, "test-namespace", pipeline.Namespace)
	assert.Equal(t, "s-test-serving-pipeline", pipeline.Name)
	assert.Equal(t, 2, len(pipeline.Spec.Vertices))
	assert.Equal(t, 1, len(pipeline.Spec.Edges))

	// Validate the source vertex
	sourceVertex := pipeline.Spec.Vertices[0]
	assert.NotNil(t, sourceVertex.Source)
	assert.NotNil(t, sourceVertex.Source.JetStream)
	assert.Equal(t, "nats://test-url", sourceVertex.Source.JetStream.URL)
	assert.Equal(t, "serving-source-test-serving-pipeline", sourceVertex.Source.JetStream.Stream)

	// Validate the sink vertex
	sinkVertex := pipeline.Spec.Vertices[1]
	assert.NotNil(t, sinkVertex.Sink)

	// Validate environment variables
	envVars := sourceVertex.ContainerTemplate.Env
	assert.Contains(t, envVars, corev1.EnvVar{Name: "NUMAFLOW_SERVING_SOURCE_SETTINGS", Value: "eyJhdXRoIjpudWxsLCJzZXJ2aWNlIjp0cnVlLCJtc2dJREhlYWRlcktleSI6bnVsbCwiY29udGFpbmVyVGVtcGxhdGUiOnsicmVzb3VyY2VzIjp7InJlcXVlc3RzIjp7Im1lbW9yeSI6IjE5NDJNaSJ9fSwiZW52IjpbeyJuYW1lIjoiVEVTVF9FTlYiLCJ2YWx1ZSI6InRlc3QtdmFsdWUifV19fQ=="})
	assert.Contains(t, envVars, corev1.EnvVar{Name: "NUMAFLOW_SERVING_KV_STORE", Value: "serving-store-test-serving-pipeline_SERVING_KV_STORE"})

	servingDeployReq := GetServingPipelineResourceReq{
		ISBSvcConfig: BufferServiceConfig{JetStream: &JetStreamConfig{URL: "nats://test-url"}},
		Image:        "quay.io/numaproj/numaflow:stable",
		PullPolicy:   corev1.PullIfNotPresent,
	}
	deploy, err := sp.GetServingDeploymentObj(servingDeployReq)
	assert.NoError(t, err)
	assert.Contains(t, deploy.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "TEST_ENV", Value: "test-value"})
	assert.Equal(t, deploy.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().String(), "1942Mi")
}
