package vertex

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// getVertexStatus calculate the status by iterating over pods objects
func getVertexStatus(pods *corev1.PodList) (string, string, bool) {
	// TODO: Need to revisit later.
	if len(pods.Items) == 0 {
		return "No Pods found", "NoPodsFound", true
	} else {
		for _, pod := range pods.Items {
			if !isContainerHealthy(&pod) {
				return fmt.Sprintf("Pod %s is not healthy", pod.Name), "CrashLoopBackOff", false
			}
		}
	}

	return "All vertex pods are healthy", "Running", true
}

func isContainerHealthy(pod *corev1.Pod) bool {
	for _, c := range pod.Status.ContainerStatuses {
		if c.State.Waiting != nil && c.State.Waiting.Reason == "CrashLoopBackOff" {
			return false
		}
	}
	return true
}
