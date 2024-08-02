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
	"fmt"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// CheckVertexPodsStatus checks the status by iterating over pods objects
func CheckVertexPodsStatus(vertexPods *corev1.PodList) (healthy bool, reason string, message string) {
	// TODO: Need to revisit later.
	if len(vertexPods.Items) == 0 {
		return true, "NoPodsFound", "No Pods found"
	} else {
		for _, pod := range vertexPods.Items {
			if podHealthy, msg := isPodHealthy(&pod); !podHealthy {
				message = fmt.Sprintf("Pod %s is unhealthy", pod.Name)
				reason = "Pod" + msg
				healthy = false
				return
			}
		}
	}
	return true, "Running", "All vertex pods are healthy"
}

func isPodHealthy(pod *corev1.Pod) (healthy bool, reason string) {
	for _, c := range pod.Status.ContainerStatuses {
		if c.State.Waiting != nil && c.State.Waiting.Reason == "CrashLoopBackOff" {
			return false, c.State.Waiting.Reason
		}
	}
	return true, ""
}

// CheckVertexStatus will calculate the status of the vertices and return the status and reason
func CheckVertexStatus(vertices *dfv1.VertexList) (bool, string) {
	for _, vertex := range vertices.Items {
		if vertex.Status.ObservedGeneration == 0 || vertex.Generation > vertex.Status.ObservedGeneration {
			return false, "Progressing"
		}
		if !vertex.Status.IsHealthy() {
			return false, "Unavailable"
		}
	}
	return true, "Healthy"
}

// CheckDeploymentStatus returns a message describing deployment status, and message with reason where bool value
// indicating if the status is considered done.
// Borrowed at kubernetes/kubectl/rollout_status.go https://github.com/kubernetes/kubernetes/blob/cea1d4e20b4a7886d8ff65f34c6d4f95efcb4742/staging/src/k8s.io/kubectl/pkg/polymorphichelpers/rollout_status.go#L59
func CheckDeploymentStatus(deployment *appv1.Deployment) (done bool, reason string, message string) {
	if deployment.Generation <= deployment.Status.ObservedGeneration {
		cond := getDeploymentCondition(deployment.Status, appv1.DeploymentProgressing)
		if cond != nil && cond.Reason == "ProgressDeadlineExceeded" {
			return false, "ProgressDeadlineExceeded", fmt.Sprintf("deployment %q exceeded its progress deadline", deployment.Name)
		}
		if deployment.Spec.Replicas != nil && deployment.Status.UpdatedReplicas < *deployment.Spec.Replicas {
			return false, "Progressing", fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d out of %d new replicas have been updated...\n",
				deployment.Name, deployment.Status.UpdatedReplicas, *deployment.Spec.Replicas)
		}
		if deployment.Status.Replicas > deployment.Status.UpdatedReplicas {
			return false, "Progressing", fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d old replicas are pending termination...\n",
				deployment.Name, deployment.Status.Replicas-deployment.Status.UpdatedReplicas)
		}
		if deployment.Status.AvailableReplicas < deployment.Status.UpdatedReplicas {
			return false, "Progressing", fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d of %d updated replicas are available...\n",
				deployment.Name, deployment.Status.AvailableReplicas, deployment.Status.UpdatedReplicas)
		}
		return true, "Healthy", fmt.Sprintf("deployment %q successfully rolled out\n", deployment.Name)
	}
	return false, "Progressing", "Waiting for deployment spec update to be observed..."
}

// GetDeploymentCondition returns the condition with the provided type.
func getDeploymentCondition(status appv1.DeploymentStatus, condType appv1.DeploymentConditionType) *appv1.DeploymentCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}
