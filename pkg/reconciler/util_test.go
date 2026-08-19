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

func readyContainer(name string) corev1.ContainerStatus {
	return corev1.ContainerStatus{
		Name:  name,
		Ready: true,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
	}
}

func TestCheckVertexPodsStatus(t *testing.T) {
	t.Run("Test Vertex status as true", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					readyContainer("numa"),
				},
				InitContainerStatuses: []corev1.ContainerStatus{
					{Name: "init", Ready: true, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"}}},
				},
			}},
		}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 1)
		assert.Equal(t, "All pods are healthy", message)
		assert.Equal(t, "Running", reason)
		assert.True(t, done)
	})

	t.Run("Test Vertex status as false", func(t *testing.T) {
		pods := corev1.PodList{
			Items: []corev1.Pod{
				{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{Name: "numa", Ready: false, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
					}},
				},
			},
		}
		done, reason, message, transient := CheckPodsStatusWithReadiness(&pods, 1)
		assert.Equal(t, `Pod test-pod: container "numa" CrashLoopBackOff`, message)
		assert.Equal(t, "PodCrashLoopBackOff", reason)
		assert.False(t, done)
		assert.False(t, transient)
	})

	t.Run("empty pods with desired>0 is unhealthy", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 1)
		assert.Equal(t, "0/1 pods are ready", message)
		assert.Equal(t, "InsufficientReadyPods", reason)
		assert.False(t, done)
	})

	t.Run("desired zero is healthy without pods", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 0)
		assert.True(t, done)
		assert.Equal(t, "NoPodsFound", reason)
		assert.Equal(t, "No Pods found", message)
	})

	t.Run("Running but not Ready is unhealthy", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "not-ready"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "numa", Ready: false, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				},
			}},
		}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 1)
		assert.False(t, done)
		assert.Equal(t, "InsufficientReadyPods", reason)
		assert.Equal(t, "0/1 pods are ready", message)
	})

	t.Run("Running without container statuses is unhealthy", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "missing-status"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			}},
		}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 1)
		assert.False(t, done)
		assert.Equal(t, "InsufficientReadyPods", reason)
		assert.Equal(t, "0/1 pods are ready", message)
	})

	t.Run("Test Vertex status as true with non-recent restart", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:  "numa",
						Ready: true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						LastTerminationState: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								FinishedAt: metav1.Time{Time: time.Now().Add(-3 * time.Minute)},
								ExitCode:   137,
								Reason:     "OOMKilled",
							},
						},
					},
				}},
			}},
		}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 1)
		assert.Equal(t, "All pods are healthy", message)
		assert.Equal(t, "Running", reason)
		assert.True(t, done)
	})

	t.Run("Test Vertex status as false with recent restart", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:  "numa",
						Ready: true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						LastTerminationState: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								FinishedAt: metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
								Reason:     "OOMKilled",
								ExitCode:   137,
							},
						},
					},
				}},
			}},
		}
		done, reason, message, transient := CheckPodsStatusWithReadiness(&pods, 1)
		assert.Equal(t, `Pod test-pod: container "numa" restarted recently: OOMKilled (exit code 137)`, message)
		assert.Equal(t, "PodRecentRestart", reason)
		assert.False(t, done)
		assert.True(t, transient)
	})

	t.Run("Pending pod with empty containerStatuses is unhealthy", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "pending-pod"}, Status: corev1.PodStatus{
				Phase: corev1.PodPending,
			}},
		}}
		done, reason, message, transient := CheckPodsStatusWithReadiness(&pods, 1)
		assert.False(t, done)
		assert.Equal(t, "PodPending", reason)
		assert.Equal(t, "Pod pending-pod: Pod is in Pending phase", message)
		assert.False(t, transient)
	})

	t.Run("Pending unschedulable pod includes PodScheduled message", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "unschedulable-pod"}, Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				Conditions: []corev1.PodCondition{
					{
						Type:    corev1.PodScheduled,
						Status:  corev1.ConditionFalse,
						Reason:  "Unschedulable",
						Message: "0/1 nodes are available: 1 Insufficient cpu. no new claims to deallocate, preemption: 0/1 nodes are available: 1 Preemption is not helpful for scheduling.",
					},
				},
			}},
		}}
		done, reason, message, transient := CheckPodsStatusWithReadiness(&pods, 1)
		assert.False(t, done)
		assert.Equal(t, "PodUnschedulable", reason)
		assert.Equal(t, "Pod unschedulable-pod cannot be scheduled: 0/1 nodes are available: 1 Insufficient cpu.", message)
		assert.False(t, transient)
	})

	t.Run("Unknown pod with empty containerStatuses is unhealthy", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "unknown-pod"}, Status: corev1.PodStatus{
				Phase: corev1.PodUnknown,
			}},
		}}
		done, reason, message, transient := CheckPodsStatusWithReadiness(&pods, 1)
		assert.False(t, done)
		assert.Equal(t, "InsufficientReadyPods", reason)
		assert.Equal(t, "0/1 pods are ready", message)
		assert.False(t, transient)
	})

	t.Run("Failed pod is unhealthy", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "failed-pod"}, Status: corev1.PodStatus{
				Phase:   corev1.PodFailed,
				Message: "Pod failed for testing",
			}},
		}}
		done, reason, message, transient := CheckPodsStatusWithReadiness(&pods, 1)
		assert.False(t, done)
		assert.Equal(t, "InsufficientReadyPods", reason)
		assert.Equal(t, "0/1 pods are ready", message)
		assert.False(t, transient)
	})

	t.Run("skip terminating pods when desired is zero", func(t *testing.T) {
		now := metav1.Now()
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "terminating-pod", DeletionTimestamp: &now}, Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
				}},
			},
		}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 0)
		assert.True(t, done)
		assert.Equal(t, "Running", reason)
		assert.Equal(t, "All pods are healthy", message)
	})

	t.Run("only terminating pods with desired>0 is unhealthy", func(t *testing.T) {
		now := metav1.Now()
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "terminating-pod", DeletionTimestamp: &now}, Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{readyContainer("numa")},
			}},
		}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 1)
		assert.False(t, done)
		assert.Equal(t, "InsufficientReadyPods", reason)
		assert.Equal(t, "0/1 pods are ready", message)
	})

	t.Run("Test skip pod with Succeeded phase when desired zero", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "succeeded-pod"}, Status: corev1.PodStatus{
				Phase: corev1.PodSucceeded,
				ContainerStatuses: []corev1.ContainerStatus{
					{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Completed"}}},
				}},
			},
		}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 0)
		assert.True(t, done)
		assert.Equal(t, "Running", reason)
		assert.Equal(t, "All pods are healthy", message)
	})

	t.Run("Test failed pod is still considered unhealthy", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "failed-pod"}, Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "numa", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error"}}},
				}},
			},
		}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 1)
		assert.False(t, done)
		assert.Equal(t, "PodError", reason)
		assert.Equal(t, `Pod failed-pod: container "numa" Error`, message)
	})

	t.Run("Test unhealthy pod is still detected among terminating pods", func(t *testing.T) {
		now := metav1.Now()
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "terminating-pod", DeletionTimestamp: &now}, Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
				}},
			},
			{ObjectMeta: metav1.ObjectMeta{Name: "unhealthy-pod"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "numa", Ready: false, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
				}},
			},
		}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 1)
		assert.False(t, done)
		assert.Equal(t, "PodCrashLoopBackOff", reason)
		assert.Equal(t, `Pod unhealthy-pod: container "numa" CrashLoopBackOff`, message)
	})

	t.Run("quorum: enough Ready pods despite extra Pending", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "ready-a"}, Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{readyContainer("nats")},
			}},
			{ObjectMeta: metav1.ObjectMeta{Name: "ready-b"}, Status: corev1.PodStatus{
				Phase:             corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{readyContainer("nats")},
			}},
			{ObjectMeta: metav1.ObjectMeta{Name: "pending"}, Status: corev1.PodStatus{
				Phase: corev1.PodPending,
			}},
		}}
		done, reason, message, _ := CheckPodsStatusWithReadiness(&pods, 2)
		assert.True(t, done)
		assert.Equal(t, "Running", reason)
		assert.Equal(t, "All pods are healthy", message)
	})

	t.Run("Test vertex status as false with failed initContainer", func(t *testing.T) {
		pods := corev1.PodList{
			Items: []corev1.Pod{
				{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{Name: "numa", Ready: false, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}},
					},
					InitContainerStatuses: []corev1.ContainerStatus{
						{Name: "init", Ready: false, State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error"}}},
					},
				}},
			},
		}
		done, reason, message, transient := CheckPodsStatusWithReadiness(&pods, 1)
		assert.Equal(t, `Pod test-pod: container "init" Error`, message)
		assert.Equal(t, "PodError", reason)
		assert.False(t, done)
		assert.False(t, transient)
	})
}

func TestSummarizeSchedulingMessage(t *testing.T) {
	tests := []struct {
		name     string
		message  string
		expected string
	}{
		{
			name:     "verbose scheduler message",
			message:  "0/1 nodes are available: 1 Insufficient cpu. preemption: 0/1 nodes are available.",
			expected: "0/1 nodes are available: 1 Insufficient cpu.",
		},
		{
			name:     "message without sentence delimiter",
			message:  "Pod is unschedulable",
			expected: "Pod is unschedulable",
		},
		{
			name:     "empty message",
			message:  "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, summarizeSchedulingMessage(tt.message))
		})
	}
}

func TestCheckPodsStatusFailureDetail(t *testing.T) {
	t.Run("OOMKilled surfaced for crash-looping container", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "mvtx-0"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:  "numa",
						Ready: false,
						State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff", Message: "back-off restarting failed container"}},
						LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
							Reason: "OOMKilled", ExitCode: 137, FinishedAt: metav1.Time{Time: time.Now().Add(-30 * time.Second)},
						}},
					},
				},
			}},
		}}
		done, reason, message, transient := CheckPodsStatus(&pods)
		assert.False(t, done)
		assert.Equal(t, "PodCrashLoopBackOff", reason)
		assert.Contains(t, message, `container "numa" CrashLoopBackOff`)
		assert.Contains(t, message, "OOMKilled")
		assert.Contains(t, message, "exit code 137")
		assert.False(t, transient)
	})

	t.Run("ImagePullBackOff includes waiting message", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "mvtx-0"}, Status: corev1.PodStatus{
				Phase: corev1.PodPending,
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "udf", Ready: false, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: `Back-off pulling image "bad:img"`}}},
				},
			}},
		}}
		done, reason, message, _ := CheckPodsStatus(&pods)
		assert.False(t, done)
		assert.Equal(t, "PodImagePullBackOff", reason)
		assert.Contains(t, message, "ImagePullBackOff")
		assert.Contains(t, message, `Back-off pulling image "bad:img"`)
	})

	t.Run("aggregates all failed containers in a pod", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "mvtx-0"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: "udf", Ready: false, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
					{Name: "numa", Ready: false, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
				},
			}},
		}}
		done, reason, message, _ := CheckPodsStatus(&pods)
		assert.False(t, done)
		assert.Equal(t, "PodImagePullBackOff", reason)
		assert.Contains(t, message, `container "udf" ImagePullBackOff`)
		assert.Contains(t, message, `container "numa" CrashLoopBackOff`)
		assert.Contains(t, message, "; ")
	})

	t.Run("recent OOM restart surfaces OOM detail", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "mvtx-0"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:  "numa",
						Ready: true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
							Reason: "OOMKilled", ExitCode: 137, FinishedAt: metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						}},
					},
				},
			}},
		}}
		done, reason, message, transient := CheckPodsStatus(&pods)
		assert.False(t, done)
		assert.Equal(t, "PodRecentRestart", reason)
		assert.True(t, transient)
		assert.Contains(t, message, "OOMKilled")
		assert.Contains(t, message, "exit code 137")
	})

	t.Run("aggregates multiple recently-restarted containers", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "mvtx-0"}, Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
				ContainerStatuses: []corev1.ContainerStatus{
					{
						Name:  "numa",
						Ready: true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
							Reason: "OOMKilled", ExitCode: 137, FinishedAt: metav1.Time{Time: time.Now().Add(-30 * time.Second)},
						}},
					},
					{
						Name:  "udf",
						Ready: true,
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
							Reason: "OOMKilled", ExitCode: 137, FinishedAt: metav1.Time{Time: time.Now().Add(-1 * time.Minute)},
						}},
					},
				},
			}},
		}}
		done, reason, message, transient := CheckPodsStatus(&pods)
		assert.False(t, done)
		assert.Equal(t, "PodRecentRestart", reason)
		assert.True(t, transient)
		assert.Contains(t, message, `container "numa" restarted recently: OOMKilled (exit code 137)`)
		assert.Contains(t, message, `container "udf" restarted recently: OOMKilled (exit code 137)`)
		assert.Contains(t, message, "; ")
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
	t.Run("Test Vertex status returns detailed message", func(t *testing.T) {
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
						Message:            "failed to connect to source",
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
		assert.Equal(t, `Vertex "test-vertex" error: failed to connect to source`, message)
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
		status, reason, msg := CheckStatefulSetStatus(testSts, 3)
		assert.Equal(t, "Healthy", reason)
		assert.True(t, status)
		assert.Equal(t, "statefulset rolling update complete 3 pods at revision isbsvc-default-js-597b7f74d7...\n", msg)
	})

	t.Run("Test statefulset status as false", func(t *testing.T) {
		testSts := statefulSet.DeepCopy()
		testSts.Status.UpdateRevision = "isbsvc-default-js-597b7f73a1"
		status, reason, msg := CheckStatefulSetStatus(testSts, 3)
		assert.Equal(t, "Progressing", reason)
		assert.False(t, status)
		assert.Equal(t, "waiting for statefulset rolling update to complete 3 pods at revision isbsvc-default-js-597b7f73a1...", msg)
	})

	t.Run("Test statefulset with ObservedGeneration as zero", func(t *testing.T) {
		testSts := statefulSet.DeepCopy()
		testSts.Status.ObservedGeneration = 0
		status, reason, msg := CheckStatefulSetStatus(testSts, 3)
		assert.Equal(t, "Progressing", reason)
		assert.False(t, status)
		assert.Equal(t, "Waiting for statefulset spec update to be observed...", msg)
	})

	t.Run("Test 3-replica statefulset healthy at quorum (2/3 ready)", func(t *testing.T) {
		replicas := int32(3)
		testSts := statefulSet.DeepCopy()
		testSts.Spec.Replicas = &replicas
		testSts.Status.ReadyReplicas = 2
		// quorum = ⌊3/2⌋+1 = 2
		status, reason, msg := CheckStatefulSetStatus(testSts, 2)
		assert.True(t, status)
		assert.Equal(t, "Healthy", reason)
		assert.Contains(t, msg, "statefulset rolling update complete")
	})

	t.Run("Test 3-replica statefulset unhealthy below quorum (1/3 ready)", func(t *testing.T) {
		replicas := int32(3)
		testSts := statefulSet.DeepCopy()
		testSts.Spec.Replicas = &replicas
		testSts.Status.ReadyReplicas = 1
		// quorum = ⌊3/2⌋+1 = 2; 1 ready < 2 quorum → unhealthy
		status, reason, msg := CheckStatefulSetStatus(testSts, 2)
		assert.False(t, status)
		assert.Equal(t, "Unavailable", reason)
		assert.Contains(t, msg, "Waiting for 1 pods to be ready")
	})

	t.Run("Test 1-replica statefulset all-or-nothing (0/1 ready)", func(t *testing.T) {
		replicas := int32(1)
		testSts := statefulSet.DeepCopy()
		testSts.Spec.Replicas = &replicas
		testSts.Status.ReadyReplicas = 0
		// quorum = ⌊1/2⌋+1 = 1; single replica must be ready
		status, reason, msg := CheckStatefulSetStatus(testSts, 1)
		assert.False(t, status)
		assert.Equal(t, "Unavailable", reason)
		assert.Contains(t, msg, "Waiting for 1 pods to be ready")
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
					InitContainerStatuses: []corev1.ContainerStatus{
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
					InitContainerStatuses: []corev1.ContainerStatus{
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
					InitContainerStatuses: []corev1.ContainerStatus{
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
					InitContainerStatuses: []corev1.ContainerStatus{
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
					InitContainerStatuses: []corev1.ContainerStatus{
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
			{
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Ready: true,
						},
					},
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							Ready: false,
						},
					},
				},
			},
		},
	}
	assert.Equal(t, 2, NumOfReadyPods(pods))
}
