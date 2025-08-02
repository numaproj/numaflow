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
	"time"

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
		done, reason, message, _ := CheckPodsStatus(&pods)
		assert.Equal(t, "All pods are healthy", message)
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
		done, reason, message, transient := CheckPodsStatus(&pods)
		assert.Equal(t, "Pod test-pod is unhealthy", message)
		assert.Equal(t, "PodCrashLoopBackOff", reason)
		assert.False(t, done)
		assert.False(t, transient)
	})

	t.Run("Test Vertex status as false with no pods", func(t *testing.T) {
		pods := corev1.PodList{
			Items: []corev1.Pod{},
		}
		done, reason, message, _ := CheckPodsStatus(&pods)
		assert.Equal(t, "No Pods found", message)
		assert.Equal(t, "NoPodsFound", reason)
		assert.True(t, done)
	})

	t.Run("Test Vertex status as true with non-recent restart", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "Running"}},
						LastTerminationState: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								FinishedAt: metav1.Time{
									Time: time.Now().Add(-3 * time.Minute),
								},
							},
						},
					},
				}},
			}},
		}
		done, reason, message, _ := CheckPodsStatus(&pods)
		assert.Equal(t, "All pods are healthy", message)
		assert.Equal(t, "Running", reason)
		assert.True(t, done)
	})

	t.Run("Test Vertex status as false with recent restart", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "Running"}},
						LastTerminationState: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								FinishedAt: metav1.Time{
									Time: time.Now().Add(-1 * time.Minute),
								},
								ExitCode: 137,
							},
						},
					},
				}},
			}},
		}
		done, reason, message, transient := CheckPodsStatus(&pods)
		assert.Equal(t, "Pod test-pod is unhealthy", message)
		assert.Equal(t, "PodRecentRestart", reason)
		assert.False(t, done)
		assert.True(t, transient)
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
		assert.Equal(t, "Healthy", reason)
		assert.True(t, done)
		assert.Equal(t, "deployment \"test-deployment\" successfully rolled out", message)
	})

	t.Run("Test Deployment status as false", func(t *testing.T) {
		testDeployment := deployment.DeepCopy()
		testDeployment.Status.ObservedGeneration = 0
		testDeployment.Status.UpdatedReplicas = 0
		done, reason, message := CheckDeploymentStatus(testDeployment)
		assert.Equal(t, "Progressing", reason)
		assert.False(t, done)
		assert.Equal(t, "Waiting for deployment \"test-deployment\" rollout to finish: 0 out of 1 new replicas have been updated...", message)
	})

	t.Run("Test deployment status as false while updating replica", func(t *testing.T) {
		testDeployment := deployment.DeepCopy()
		testDeployment.Status.UpdatedReplicas = 1
		testDeployment.Status.Replicas = 2
		done, reason, message := CheckDeploymentStatus(testDeployment)
		assert.Equal(t, "Progressing", reason)
		assert.False(t, done)
		assert.Equal(t, "Waiting for deployment \"test-deployment\" rollout to finish: 1 old replicas are pending termination...", message)
	})
}

func TestGetVertexStatus(t *testing.T) {
	t.Run("Test Vertex status as true", func(t *testing.T) {
		vertices := dfv1.VertexList{
			Items: []dfv1.Vertex{
				{
					ObjectMeta: metav1.ObjectMeta{
						Generation: 1,
					},
					Status: dfv1.VertexStatus{
						Phase:              "Running",
						ObservedGeneration: 1,
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
		status, reason, message := CheckVertexStatus(&vertices)
		assert.True(t, status)
		assert.Equal(t, "Healthy", reason)
		assert.Equal(t, "All vertices are healthy", message)
	})

	t.Run("Test Vertex status as false when ObservedGeneration is not matching", func(t *testing.T) {
		vertices := dfv1.VertexList{
			Items: []dfv1.Vertex{
				{
					ObjectMeta: metav1.ObjectMeta{
						Generation: 2,
					},
					Spec: dfv1.VertexSpec{
						AbstractVertex: dfv1.AbstractVertex{
							Name: "test-vertex",
						},
					},
					Status: dfv1.VertexStatus{
						Phase:              "Running",
						ObservedGeneration: 1,
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
		status, reason, message := CheckVertexStatus(&vertices)
		assert.False(t, status)
		assert.Equal(t, "Progressing", reason)
		assert.Equal(t, `Vertex "test-vertex" Waiting for reconciliation`, message)
	})

	t.Run("Test Vertex status as false", func(t *testing.T) {
		vertices := dfv1.VertexList{
			Items: []dfv1.Vertex{
				{
					ObjectMeta: metav1.ObjectMeta{
						Generation: 2,
					},
					Spec: dfv1.VertexSpec{
						AbstractVertex: dfv1.AbstractVertex{
							Name: "test-vertex",
						},
					},
					Status: dfv1.VertexStatus{
						Phase:              "Pending",
						ObservedGeneration: 2,
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
		status, reason, message := CheckVertexStatus(&vertices)
		assert.False(t, status)
		assert.Equal(t, "Unavailable", reason)
		assert.Equal(t, `Vertex "test-vertex" is not healthy`, message)
	})
}

var (
	statefulSet = &appv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-statefulset",
			Namespace: "default",
		},
		Status: appv1.StatefulSetStatus{
			AvailableReplicas:  3,
			CurrentReplicas:    3,
			CurrentRevision:    "isbsvc-default-js-597b7f74d7",
			ObservedGeneration: 1,
			ReadyReplicas:      3,
			Replicas:           3,
			UpdateRevision:     "isbsvc-default-js-597b7f74d7",
			UpdatedReplicas:    3,
		},
	}
)

func TestGetStatefulSetStatus(t *testing.T) {
	t.Run("Test statefulset status as true", func(t *testing.T) {
		testSts := statefulSet.DeepCopy()
		status, reason, msg := CheckStatefulSetStatus(testSts)
		assert.Equal(t, "Healthy", reason)
		assert.True(t, status)
		assert.Equal(t, "statefulset rolling update complete 3 pods at revision isbsvc-default-js-597b7f74d7...\n", msg)
	})

	t.Run("Test statefulset status as false", func(t *testing.T) {
		testSts := statefulSet.DeepCopy()
		testSts.Status.UpdateRevision = "isbsvc-default-js-597b7f73a1"
		status, reason, msg := CheckStatefulSetStatus(testSts)
		assert.Equal(t, "Progressing", reason)
		assert.False(t, status)
		assert.Equal(t, "waiting for statefulset rolling update to complete 3 pods at revision isbsvc-default-js-597b7f73a1...", msg)
	})

	t.Run("Test statefulset with ObservedGeneration as zero", func(t *testing.T) {
		testSts := statefulSet.DeepCopy()
		testSts.Status.ObservedGeneration = 0
		status, reason, msg := CheckStatefulSetStatus(testSts)
		assert.Equal(t, "Progressing", reason)
		assert.False(t, status)
		assert.Equal(t, "Waiting for statefulset spec update to be observed...", msg)
	})
}

func TestNumOfReadyPods(t *testing.T) {
	pods := corev1.PodList{
		Items: []corev1.Pod{
			{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Ready: true,
						},
						{
							Ready: true,
						},
					},
				},
			},
			{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Ready: false,
						},
						{
							Ready: true,
						},
					},
				},
			},
			{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Ready: true,
						},
						{
							Ready: false,
						},
					},
				},
			},
			{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Ready: true,
						},
						{
							Ready: true,
						},
						{
							Ready: true,
						},
					},
				},
			},
			{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Ready: false,
						},
						{
							Ready: false,
						},
						{
							Ready: false,
						},
					},
				},
			},
			{
				Status: corev1.PodStatus{
					Phase: corev1.PodFailed,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Ready: true,
						},
						{
							Ready: true,
						},
						{
							Ready: true,
						},
					},
				},
			},
		},
	}
	assert.Equal(t, 2, NumOfReadyPods(pods))
}
