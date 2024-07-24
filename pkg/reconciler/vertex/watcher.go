package vertex

import (
	"fmt"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// getVertexStatus calculate the status by iterating over pods objects
func getVertexStatus(vertex *dfv1.Vertex, pods *corev1.PodList) (string, string, bool) {
	if len(pods.Items) == 0 {
		return "No Pods found", "CooldownPeriod", true
	} else if *vertex.Spec.Replicas != int32(len(pods.Items)) {
		return "Number of pods are not equal to replicas", "Processing", false
	} else {
		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodRunning {
				return fmt.Sprintf("Pod %s is not in running state", pod.Name), "Processing", false
			}
		}
	}

	return "All vertex pods are healthy", "Running", true
}
