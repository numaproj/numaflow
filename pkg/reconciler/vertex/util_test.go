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

package vertex

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetVertexStatus(t *testing.T) {
	t.Run("Test Vertex status as true", func(t *testing.T) {
		pods := corev1.PodList{Items: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}, Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "Running"}}},
				}},
			}},
		}
		message, reason, done := getVertexStatus(&pods)
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
		message, reason, done := getVertexStatus(&pods)
		assert.Equal(t, "Pod test-pod is not healthy", message)
		assert.Equal(t, "CrashLoopBackOff", reason)
		assert.False(t, done)
	})

	t.Run("Test Vertex status as false with no pods", func(t *testing.T) {
		pods := corev1.PodList{
			Items: []corev1.Pod{},
		}
		message, reason, done := getVertexStatus(&pods)
		assert.Equal(t, "No Pods found", message)
		assert.Equal(t, "NoPodsFound", reason)
		assert.True(t, done)
	})
}
