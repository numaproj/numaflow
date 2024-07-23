package vertex

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func setPodStatus(c client.Client, vertex *dfv1.Vertex) error {
	// fetch the pods for calculating the status of child resources
	var podList corev1.PodList
	if err := c.List(context.TODO(), &podList,
		client.InNamespace(vertex.GetNamespace()),
		client.MatchingLabels{dfv1.KeyAppName: vertex.GetName()},
	); err != nil {
		return err
	}

	if msg, reason, status := getVertexStatus(vertex, &podList); status {
		vertex.Status.MarkPodHealthy(reason, msg)
	} else {
		vertex.Status.MarkPodNotHealthy(reason, msg)
	}

	return nil
}

// getVertexStatus calculate the status by iterating over pods objects
// TODO: Handle "Cooldown period" for the vertex pods where no any pod is available.
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
