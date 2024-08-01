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

package reconciler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func TestCheckVertexPodsStatus(t *testing.T) {
	t.Run("Test Vertex status as true", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "Running"}}},
				}},
			}},
		}
		done, reason, message := CheckVertexPodsStatus(&pods)
		assert.Equal(t, "All vertex pods are healthy", message)
		assert.Equal(t, "Running", reason)
		assert.True(t, done)
	})

	t.Run("Test Vertex status as false", func(t *testing.T) {
		pods := corev1.PodList{
			Items: []corev1.Pod{
				{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
					}},
				},
			},
		}
		done, reason, message := CheckVertexPodsStatus(&pods)
		assert.Equal(t, "Pod test-pod is unhealthy", message)
		assert.Equal(t, "PodCrashLoopBackOff", reason)
		assert.False(t, done)
	})

	t.Run("Test Vertex status as false with no pods", func(t *testing.T) {
		pods := corev1.PodList{
			Items: []corev1.Pod{},
		}
		done, reason, message := CheckVertexPodsStatus(&pods)
		assert.Equal(t, "No Pods found", message)
		assert.Equal(t, "NoPodsFound", reason)
		assert.True(t, done)
	})
}

var (
	replicas   int32 = 1
	deployment       = appv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment",
			Namespace: "default",
		},
		Spec: appv1.DeploymentSpec{
			Replicas: &replicas,
		},
		Status: appv1.DeploymentStatus{
			ObservedGeneration: 1,
			UpdatedReplicas:    1,
			Replicas:           1,
			AvailableReplicas:  1,
		},
	}
)

func TestGetDeploymentStatus(t *testing.T) {
	t.Run("Test Deployment status as true", func(t *testing.T) {
		testDeployment := deployment.DeepCopy()
		done, reason, message := CheckDeploymentStatus(testDeployment)
		assert.Equal(t, "DeploymentComplete", reason)
		assert.True(t, done)
		assert.Equal(t, "deployment \"test-deployment\" successfully rolled out\n", message)
	})

	t.Run("Test Deployment status as false", func(t *testing.T) {
		testDeployment := deployment.DeepCopy()
		testDeployment.Status.ObservedGeneration = 0
		testDeployment.Status.UpdatedReplicas = 0
		done, reason, message := CheckDeploymentStatus(testDeployment)
		assert.Equal(t, "DeploymentNotComplete", reason)
		assert.False(t, done)
		assert.Equal(t, "Waiting for deployment \"test-deployment\" rollout to finish: 0 out of 1 new replicas have been updated...\n", message)
	})

	t.Run("Test deployment status as false while updating replica", func(t *testing.T) {
		testDeployment := deployment.DeepCopy()
		testDeployment.Status.UpdatedReplicas = 1
		testDeployment.Status.Replicas = 2
		done, reason, message := CheckDeploymentStatus(testDeployment)
		assert.Equal(t, "DeploymentNotComplete", reason)
		assert.False(t, done)
		assert.Equal(t, "Waiting for deployment \"test-deployment\" rollout to finish: 1 old replicas are pending termination...\n", message)
	})
}

func TestGetVertexStatus(t *testing.T) {
	t.Run("Test Vertex status as true", func(t *testing.T) {
		vertices := dfv1.VertexList{
			Items: []dfv1.Vertex{
				{
					Status: dfv1.VertexStatus{
						Phase: "Running",
					},
				},
			},
		}
		vertices.Items[0].Status.Conditions = []metav1.Condition{
			{
				Type:   string(dfv1.VertexConditionPodsHealthy),
				Status: metav1.ConditionTrue,
			},
		}
		status, reason := CheckVertexStatus(&vertices)
		assert.True(t, status)
		assert.Equal(t, "Successful", reason)
	})

	t.Run("Test Vertex status as false", func(t *testing.T) {
		vertices := dfv1.VertexList{
			Items: []dfv1.Vertex{
				{
					Status: dfv1.VertexStatus{
						Phase: "Pending",
					},
				},
			},
		}
		vertices.Items[0].Status.Conditions = []metav1.Condition{
			{
				Type:   string(dfv1.VertexConditionPodsHealthy),
				Status: metav1.ConditionTrue,
			},
		}
		status, reason := CheckVertexStatus(&vertices)
		assert.False(t, status)
		assert.Equal(t, "Progressing", reason)
	})
}
