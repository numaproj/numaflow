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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

var (
	testMvtx = MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
		Spec: MonoVertexSpec{
			Scale: Scale{
				Min: ptr.To[int32](2),
				Max: ptr.To[int32](4),
			},
			Source: &Source{
				UDSource: &UDSource{
					Container: &Container{
						Image: "test-image1",
					},
				},
				UDTransformer: &UDTransformer{
					Container: &Container{
						Image: "test-image2",
					},
				},
			},
			Sink: &Sink{
				AbstractSink: AbstractSink{
					UDSink: &UDSink{
						Container: &Container{
							Image: "test-image3",
						},
					},
				},
			},
		},
	}
)

func TestMonoVertex_GetDaemonServiceObj(t *testing.T) {
	mv := MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
		},
	}

	svc := mv.GetDaemonServiceObj()
	if svc.Name != "test-mv-daemon-svc" {
		t.Error("GetDaemonServiceObj generated incorrect service name")
	}
	if svc.Namespace != "default" {
		t.Error("GetDaemonServiceObj generated incorrect namespace")
	}
}

func TestMonoVertex_MarkPhaseRunning(t *testing.T) {
	mvs := MonoVertexStatus{}
	mvs.MarkPhaseRunning()

	if mvs.Phase != MonoVertexPhaseRunning {
		t.Errorf("MarkPhaseRunning did not set the Phase to Running, got %v", mvs.Phase)
	}
}

func TestMonoVertex_IsHealthy(t *testing.T) {
	mvs := MonoVertexStatus{}

	mvs.InitConditions()
	mvs.MarkPhaseRunning()
	mvs.MarkDeployed()
	mvs.MarkDaemonHealthy()
	mvs.MarkPodHealthy("AllGood", "All pod are up and running")

	isHealthy := mvs.IsHealthy()
	if !isHealthy {
		t.Error("IsHealthy should return true when everything is healthy")
	}

	mvs.MarkPodNotHealthy("PodIssue", "One of the pods is down")
	isHealthy = mvs.IsHealthy()
	if isHealthy {
		t.Error("IsHealthy should return false when pod condition is not healthy")
	}
}

func TestMonoVertexStatus_MarkDeployFailed(t *testing.T) {
	mvs := MonoVertexStatus{}
	mvs.MarkDeployFailed("DeployError", "Deployment failed due to resource constraints")

	if mvs.Phase != MonoVertexPhaseFailed {
		t.Errorf("MarkDeployFailed should set the Phase to Failed, got %v", mvs.Phase)
	}
	if mvs.Reason != "DeployError" {
		t.Errorf("MarkDeployFailed should set the Reason to 'DeployError', got %s", mvs.Reason)
	}
	if mvs.Message != "Deployment failed due to resource constraints" {
		t.Errorf("MarkDeployFailed should set the Message correctly, got %s", mvs.Message)
	}
}

func TestMonoVertexGetPodSpec(t *testing.T) {

	t.Run("test get pod spec - okay", func(t *testing.T) {
		req := GetMonoVertexPodSpecReq{
			Image:      "my-image",
			PullPolicy: corev1.PullIfNotPresent,
			Env: []corev1.EnvVar{
				{
					Name:  "ENV_VAR_NAME",
					Value: "ENV_VAR_VALUE",
				},
			},
			DefaultResources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("200Mi"),
				},
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
		}
		podSpec, err := testMvtx.GetPodSpec(req)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(podSpec.Containers))
		assert.Equal(t, 1, len(podSpec.Volumes))
		assert.Equal(t, "my-image", podSpec.Containers[0].Image)
		assert.Equal(t, corev1.PullIfNotPresent, podSpec.Containers[0].ImagePullPolicy)
		assert.Equal(t, "100m", podSpec.Containers[0].Resources.Requests.Cpu().String())
		assert.Equal(t, "200m", podSpec.Containers[0].Resources.Limits.Cpu().String())
		assert.Equal(t, "100Mi", podSpec.Containers[0].Resources.Requests.Memory().String())
		assert.Equal(t, "200Mi", podSpec.Containers[0].Resources.Limits.Memory().String())
		assert.Equal(t, "test-image1", podSpec.Containers[1].Image)
		assert.Equal(t, "test-image2", podSpec.Containers[2].Image)
		assert.Equal(t, "test-image3", podSpec.Containers[3].Image)
		for _, c := range podSpec.Containers {
			assert.Equal(t, 1, len(c.VolumeMounts))
		}
		envNames := []string{}
		for _, env := range podSpec.Containers[0].Env {
			envNames = append(envNames, env.Name)
		}
		assert.Contains(t, envNames, "ENV_VAR_NAME")
		assert.Contains(t, envNames, EnvMonoVertexObject)
	})
}
