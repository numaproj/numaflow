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
