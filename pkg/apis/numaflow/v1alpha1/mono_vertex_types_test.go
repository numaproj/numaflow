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
	"time"

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
			ContainerTemplate: &ContainerTemplate{
				ReadinessProbe: &Probe{
					InitialDelaySeconds: ptr.To[int32](24),
					PeriodSeconds:       ptr.To[int32](25),
					FailureThreshold:    ptr.To[int32](2),
					TimeoutSeconds:      ptr.To[int32](21),
				},
				LivenessProbe: &Probe{
					InitialDelaySeconds: ptr.To[int32](14),
					PeriodSeconds:       ptr.To[int32](15),
					FailureThreshold:    ptr.To[int32](1),
					TimeoutSeconds:      ptr.To[int32](11),
				},
			},
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

func TestMonoVertex_MarkPhasePaused(t *testing.T) {
	mvs := MonoVertexStatus{}
	mvs.MarkPhasePaused()

	if mvs.Phase != MonoVertexPhasePaused {
		t.Errorf("MarkPhaseRunning did not set the Phase to Paused, got %v", mvs.Phase)
	}
}

func TestMonoVertex_GetDesiredPhase(t *testing.T) {
	lc := MonoVertexLifecycle{}
	assert.Equal(t, MonoVertexPhaseRunning, lc.GetDesiredPhase())
	lc.DesiredPhase = MonoVertexPhasePaused
	assert.Equal(t, MonoVertexPhasePaused, lc.GetDesiredPhase())
}

func TestMonoVertex_MarkDaemonUnHealthy(t *testing.T) {
	mvs := MonoVertexStatus{}
	mvs.MarkDaemonUnHealthy("reason", "message")

	for _, condition := range mvs.Conditions {
		if condition.Type == string(MonoVertexConditionDaemonHealthy) {
			if condition.Status != metav1.ConditionFalse {
				t.Errorf("MarkDaemonUnHealthy should set the DaemonHealthy condition to false, got %v", condition.Status)
			}
			if condition.Reason != "reason" {
				t.Errorf("MarkDaemonUnHealthy should set the Reason to 'reason', got %s", condition.Reason)
			}
			if condition.Message != "message" {
				t.Errorf("MarkDaemonUnHealthy should set the Message to 'message', got %s", condition.Message)
			}
		}
	}
}

func TestMonoVertex_SetObservedGeneration(t *testing.T) {
	mvs := MonoVertexStatus{}
	mvs.SetObservedGeneration(1)
	assert.Equal(t, int64(1), mvs.ObservedGeneration)
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
		assert.Equal(t, 1, len(podSpec.Containers))
		assert.Equal(t, 4, len(podSpec.InitContainers))
		assert.Equal(t, 2, len(podSpec.Volumes))
		assert.Equal(t, "my-image", podSpec.Containers[0].Image)
		assert.Equal(t, corev1.PullIfNotPresent, podSpec.Containers[0].ImagePullPolicy)
		assert.Equal(t, "100m", podSpec.Containers[0].Resources.Requests.Cpu().String())
		assert.Equal(t, "200m", podSpec.Containers[0].Resources.Limits.Cpu().String())
		assert.Equal(t, "100Mi", podSpec.Containers[0].Resources.Requests.Memory().String())
		assert.Equal(t, "200Mi", podSpec.Containers[0].Resources.Limits.Memory().String())
		assert.Equal(t, CtrMonitor, podSpec.InitContainers[0].Name)
		assert.Equal(t, "test-image1", podSpec.InitContainers[1].Image)
		assert.Equal(t, "test-image2", podSpec.InitContainers[2].Image)
		assert.Equal(t, "test-image3", podSpec.InitContainers[3].Image)
		for i, c := range podSpec.Containers {
			if i != 0 {
				assert.Equal(t, 1, len(c.VolumeMounts))
			}
		}
		for i, c := range podSpec.InitContainers {
			if i == 0 {
				assert.Equal(t, 1, len(c.VolumeMounts)) // monitor container
				continue
			}
			assert.Equal(t, 2, len(c.VolumeMounts))
		}
		envNames := []string{}
		for _, env := range podSpec.Containers[0].Env {
			envNames = append(envNames, env.Name)
		}
		assert.Contains(t, envNames, "ENV_VAR_NAME")
		assert.Contains(t, envNames, EnvMonoVertexObject)
		assert.Equal(t, 2, len(podSpec.Containers[0].VolumeMounts))
		assert.NotNil(t, podSpec.Containers[0].ReadinessProbe)
		assert.Equal(t, int32(24), podSpec.Containers[0].ReadinessProbe.InitialDelaySeconds)
		assert.Equal(t, int32(25), podSpec.Containers[0].ReadinessProbe.PeriodSeconds)
		assert.Equal(t, int32(2), podSpec.Containers[0].ReadinessProbe.FailureThreshold)
		assert.Equal(t, int32(21), podSpec.Containers[0].ReadinessProbe.TimeoutSeconds)
		assert.NotNil(t, podSpec.Containers[0].LivenessProbe)
		assert.Equal(t, int32(14), podSpec.Containers[0].LivenessProbe.InitialDelaySeconds)
		assert.Equal(t, int32(15), podSpec.Containers[0].LivenessProbe.PeriodSeconds)
		assert.Equal(t, int32(1), podSpec.Containers[0].LivenessProbe.FailureThreshold)
		assert.Equal(t, int32(11), podSpec.Containers[0].LivenessProbe.TimeoutSeconds)
	})
}

func TestMonoVertexLimits_GetReadBatchSize(t *testing.T) {
	t.Run("default value", func(t *testing.T) {
		mvl := MonoVertexLimits{}
		assert.Equal(t, uint64(DefaultReadBatchSize), mvl.GetReadBatchSize())
	})

	t.Run("custom value", func(t *testing.T) {
		customSize := uint64(1000)
		mvl := MonoVertexLimits{ReadBatchSize: &customSize}
		assert.Equal(t, customSize, mvl.GetReadBatchSize())
	})

}

func TestMonoVertexLimits_GetReadTimeout(t *testing.T) {
	t.Run("default value", func(t *testing.T) {
		mvl := MonoVertexLimits{}
		assert.Equal(t, DefaultReadTimeout, mvl.GetReadTimeout())
	})

	t.Run("custom value", func(t *testing.T) {
		customTimeout := metav1.Duration{Duration: 5 * time.Second}
		mvl := MonoVertexLimits{ReadTimeout: &customTimeout}
		assert.Equal(t, 5*time.Second, mvl.GetReadTimeout())
	})
}

func TestMonoVertex_GetDaemonDeploymentName(t *testing.T) {
	mv := MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-vertex",
		},
	}
	expected := "test-vertex-mv-daemon"
	assert.Equal(t, expected, mv.GetDaemonDeploymentName())
}

func TestMonoVertex_GetDaemonServiceURL(t *testing.T) {
	mv := MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vertex",
			Namespace: "test-namespace",
		},
	}
	expected := "test-vertex-mv-daemon-svc.test-namespace.svc:4327"
	assert.Equal(t, expected, mv.GetDaemonServiceURL())
}

func TestMonoVertex_Scalable(t *testing.T) {
	t.Run("scalable when not disabled", func(t *testing.T) {
		mv := MonoVertex{
			Spec: MonoVertexSpec{
				Scale: Scale{
					Disabled: false,
				},
			},
		}
		assert.True(t, mv.Scalable())
	})

	t.Run("not scalable when disabled", func(t *testing.T) {
		mv := MonoVertex{
			Spec: MonoVertexSpec{
				Scale: Scale{
					Disabled: true,
				},
			},
		}
		assert.False(t, mv.Scalable())
	})
}

func TestMonoVertex_GetReplicas(t *testing.T) {
	t.Run("default replicas", func(t *testing.T) {
		mv := MonoVertex{}
		assert.Equal(t, 1, mv.getReplicas())
	})

	t.Run("custom replicas", func(t *testing.T) {
		replicas := int32(3)
		mv := MonoVertex{
			Spec: MonoVertexSpec{
				Replicas: &replicas,
			},
		}
		assert.Equal(t, 3, mv.getReplicas())
	})
}

func TestMonoVertex_CalculateReplicas(t *testing.T) {
	t.Run("auto scaling disabled", func(t *testing.T) {
		replicas := int32(5)
		mv := MonoVertex{
			Spec: MonoVertexSpec{
				Replicas: &replicas,
				Scale: Scale{
					Disabled: true,
				},
			},
		}
		assert.Equal(t, 5, mv.CalculateReplicas())
	})

	t.Run("auto scaling enabled, within range", func(t *testing.T) {
		replicas := int32(3)
		mv := MonoVertex{
			Spec: MonoVertexSpec{
				Replicas: &replicas,
				Scale: Scale{
					Disabled: false,
					Min:      ptr.To[int32](1),
					Max:      ptr.To[int32](5),
				},
			},
		}
		assert.Equal(t, 3, mv.CalculateReplicas())
	})

	t.Run("auto scaling enabled, below min", func(t *testing.T) {
		replicas := int32(0)
		mv := MonoVertex{
			Spec: MonoVertexSpec{
				Replicas: &replicas,
				Scale: Scale{
					Disabled: false,
					Min:      ptr.To[int32](2),
					Max:      ptr.To[int32](5),
				},
			},
		}
		assert.Equal(t, 2, mv.CalculateReplicas())
	})

	t.Run("auto scaling enabled, above max", func(t *testing.T) {
		replicas := int32(10)
		mv := MonoVertex{
			Spec: MonoVertexSpec{
				Replicas: &replicas,
				Scale: Scale{
					Disabled: false,
					Min:      ptr.To[int32](2),
					Max:      ptr.To[int32](5),
				},
			},
		}
		assert.Equal(t, 5, mv.CalculateReplicas())
	})

	t.Run("phase paused", func(t *testing.T) {
		replicas := int32(10)
		mv := MonoVertex{
			Spec: MonoVertexSpec{
				Lifecycle: MonoVertexLifecycle{DesiredPhase: MonoVertexPhasePaused},
				Replicas:  &replicas,
				Scale: Scale{
					Disabled: false,
					Min:      ptr.To[int32](2),
					Max:      ptr.To[int32](5),
				},
			},
		}
		assert.Equal(t, 0, mv.CalculateReplicas())
	})
}

func TestMonoVertex_GetServiceObj(t *testing.T) {
	mv := MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vertex",
			Namespace: "test-namespace",
		},
	}

	t.Run("non-headless service", func(t *testing.T) {
		svc := mv.getServiceObj("test-service", false, map[string]int32{"http": 8080})
		assert.Equal(t, "test-service", svc.Name)
		assert.Equal(t, "test-namespace", svc.Namespace)
		assert.Equal(t, 1, len(svc.Spec.Ports))
		assert.Equal(t, int32(8080), svc.Spec.Ports[0].Port)
		assert.Equal(t, "http", svc.Spec.Ports[0].Name)
		assert.NotEqual(t, "None", svc.Spec.ClusterIP)
	})

	t.Run("headless service", func(t *testing.T) {
		svc := mv.getServiceObj("test-headless-service", true, map[string]int32{"grpc": 9090})
		assert.Equal(t, "test-headless-service", svc.Name)
		assert.Equal(t, "test-namespace", svc.Namespace)
		assert.Equal(t, 1, len(svc.Spec.Ports))
		assert.Equal(t, int32(9090), svc.Spec.Ports[0].Port)
		assert.Equal(t, "grpc", svc.Spec.Ports[0].Name)
		assert.Equal(t, "None", svc.Spec.ClusterIP)
	})

	t.Run("verify labels", func(t *testing.T) {
		svc := mv.getServiceObj("test-label-service", false, map[string]int32{"metrics": 7070})
		expectedLabels := map[string]string{
			KeyPartOf:         Project,
			KeyManagedBy:      ControllerMonoVertex,
			KeyComponent:      ComponentMonoVertex,
			KeyMonoVertexName: "test-vertex",
		}
		assert.Equal(t, expectedLabels, svc.Labels)
	})

	t.Run("verify selector", func(t *testing.T) {
		svc := mv.getServiceObj("test-selector-service", false, map[string]int32{"admin": 6060})
		expectedSelector := map[string]string{
			KeyPartOf:         Project,
			KeyManagedBy:      ControllerMonoVertex,
			KeyComponent:      ComponentMonoVertex,
			KeyMonoVertexName: "test-vertex",
		}
		assert.Equal(t, expectedSelector, svc.Spec.Selector)
	})
}

func TestMonoVertex_GetServiceObjs(t *testing.T) {
	mv := MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vertex",
			Namespace: "test-namespace",
		},
	}

	t.Run("verify default service objects", func(t *testing.T) {
		services := mv.GetServiceObjs()
		assert.Equal(t, 1, len(services), "Expected 1 service object")

		headlessService := services[0]
		assert.Equal(t, mv.GetHeadlessServiceName(), headlessService.Name)
		assert.Equal(t, "test-namespace", headlessService.Namespace)
		assert.Equal(t, "None", headlessService.Spec.ClusterIP)
		assert.Equal(t, 2, len(headlessService.Spec.Ports))

		// Verify that the ports contain the expected values
		expectedPorts := map[int32]string{
			MonoVertexMetricsPort: MonoVertexMetricsPortName,
			MonoVertexMonitorPort: MonoVertexMonitorPortName,
		}
		foundPorts := map[int32]string{}
		for _, port := range headlessService.Spec.Ports {
			foundPorts[port.Port] = port.Name
		}
		for port, name := range expectedPorts {
			assert.Equal(t, name, foundPorts[port], "Port name mismatch for port %d", port)
		}
	})

	t.Run("verify HTTP source without service", func(t *testing.T) {
		mvCopy := mv.DeepCopy()
		mvCopy.Spec.Source = &Source{
			HTTP: &HTTPSource{},
		}
		services := mvCopy.GetServiceObjs()
		assert.Equal(t, 1, len(services), "Expected 1 service object (headless only)")
		assert.Equal(t, services[0].Name, mvCopy.GetHeadlessServiceName())
		assert.Equal(t, 2, len(services[0].Spec.Ports))

		// Verify headless service ports
		ports := map[int32]bool{
			MonoVertexMetricsPort: false,
			MonoVertexMonitorPort: false,
		}
		for _, port := range services[0].Spec.Ports {
			ports[port.Port] = true
		}
		assert.True(t, ports[MonoVertexMetricsPort], "Metrics port is missing")
		assert.True(t, ports[MonoVertexMonitorPort], "Monitor port is missing")
		assert.Equal(t, "None", services[0].Spec.ClusterIP)
	})

	t.Run("verify HTTP source with service enabled", func(t *testing.T) {
		mvCopy := mv.DeepCopy()
		mvCopy.Spec.Source = &Source{
			HTTP: &HTTPSource{
				Service: true,
			},
		}
		services := mvCopy.GetServiceObjs()
		assert.Equal(t, 2, len(services), "Expected 2 service objects (headless + HTTP)")

		// First service should be headless
		assert.Equal(t, services[0].Name, mvCopy.GetHeadlessServiceName())
		assert.Equal(t, "None", services[0].Spec.ClusterIP)

		// Second service should be the HTTP service
		assert.Equal(t, services[1].Name, mvCopy.Name)
		assert.Equal(t, 1, len(services[1].Spec.Ports))
		assert.Equal(t, VertexHTTPSPort, int(services[1].Spec.Ports[0].Port))
		assert.Equal(t, VertexHTTPSPortName, services[1].Spec.Ports[0].Name)
		assert.NotEqual(t, "None", services[1].Spec.ClusterIP)
	})
}

func TestMonoVertex_GetDaemonDeploymentObj(t *testing.T) {
	mv := MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-vertex",
			Namespace: "test-namespace",
		},
		Spec: MonoVertexSpec{},
	}

	t.Run("basic deployment object", func(t *testing.T) {
		req := GetMonoVertexDaemonDeploymentReq{
			Image:      "test-image:latest",
			PullPolicy: corev1.PullAlways,
			DefaultResources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("100m"),
					corev1.ResourceMemory: resource.MustParse("128Mi"),
				},
			},
		}

		deployment, err := mv.GetDaemonDeploymentObj(req)
		assert.NoError(t, err)
		assert.NotNil(t, deployment)
		assert.Equal(t, mv.GetDaemonDeploymentName(), deployment.Name)
		assert.Equal(t, mv.Namespace, deployment.Namespace)
		assert.Equal(t, "test-image:latest", deployment.Spec.Template.Spec.Containers[0].Image)
		assert.Equal(t, corev1.PullAlways, deployment.Spec.Template.Spec.Containers[0].ImagePullPolicy)
		assert.Equal(t, resource.MustParse("100m"), deployment.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU])
		assert.Equal(t, resource.MustParse("128Mi"), deployment.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory])
	})

	t.Run("with custom environment variables", func(t *testing.T) {
		req := GetMonoVertexDaemonDeploymentReq{
			Image: "test-image:v1",
			Env: []corev1.EnvVar{
				{Name: "CUSTOM_ENV", Value: "custom_value"},
			},
		}

		deployment, err := mv.GetDaemonDeploymentObj(req)
		assert.NoError(t, err)
		assert.NotNil(t, deployment)

		envVars := deployment.Spec.Template.Spec.Containers[0].Env
		assert.Contains(t, envVars, corev1.EnvVar{Name: "CUSTOM_ENV", Value: "custom_value"})
	})

	t.Run("with daemon template", func(t *testing.T) {
		mv.Spec.DaemonTemplate = &DaemonTemplate{
			Replicas: ptr.To[int32](3),
			AbstractPodTemplate: AbstractPodTemplate{
				NodeSelector: map[string]string{"node": "special"},
			},
			ContainerTemplate: &ContainerTemplate{
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("200m"),
					},
				},
			},
		}

		req := GetMonoVertexDaemonDeploymentReq{
			Image: "test-image:v2",
		}

		deployment, err := mv.GetDaemonDeploymentObj(req)
		assert.NoError(t, err)
		assert.NotNil(t, deployment)
		assert.Equal(t, int32(3), *deployment.Spec.Replicas)
		assert.Equal(t, "special", deployment.Spec.Template.Spec.NodeSelector["node"])
		assert.Equal(t, resource.MustParse("200m"), deployment.Spec.Template.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU])
	})

	t.Run("verify probes", func(t *testing.T) {
		req := GetMonoVertexDaemonDeploymentReq{
			Image: "test-image:v3",
		}

		deployment, err := mv.GetDaemonDeploymentObj(req)
		assert.NoError(t, err)
		assert.NotNil(t, deployment)

		container := deployment.Spec.Template.Spec.Containers[0]
		assert.NotNil(t, container.ReadinessProbe)
		assert.NotNil(t, container.LivenessProbe)

		assert.Equal(t, int32(MonoVertexDaemonServicePort), container.ReadinessProbe.HTTPGet.Port.IntVal)
		assert.Equal(t, "/readyz", container.ReadinessProbe.HTTPGet.Path)
		assert.Equal(t, corev1.URISchemeHTTPS, container.ReadinessProbe.HTTPGet.Scheme)

		assert.Equal(t, int32(MonoVertexDaemonServicePort), container.LivenessProbe.HTTPGet.Port.IntVal)
		assert.Equal(t, "/livez", container.LivenessProbe.HTTPGet.Path)
		assert.Equal(t, corev1.URISchemeHTTPS, container.LivenessProbe.HTTPGet.Scheme)
	})

	t.Run("verify labels and owner references", func(t *testing.T) {
		req := GetMonoVertexDaemonDeploymentReq{
			Image: "test-image:v4",
		}

		deployment, err := mv.GetDaemonDeploymentObj(req)
		assert.NoError(t, err)
		assert.NotNil(t, deployment)

		expectedLabels := map[string]string{
			KeyPartOf:         Project,
			KeyManagedBy:      ControllerMonoVertex,
			KeyComponent:      ComponentMonoVertexDaemon,
			KeyAppName:        mv.GetDaemonDeploymentName(),
			KeyMonoVertexName: mv.Name,
		}
		assert.Equal(t, expectedLabels, deployment.Labels)

		assert.Len(t, deployment.OwnerReferences, 1)
		assert.Equal(t, mv.Name, deployment.OwnerReferences[0].Name)
		assert.Equal(t, MonoVertexGroupVersionKind.Kind, deployment.OwnerReferences[0].Kind)
	})
}
