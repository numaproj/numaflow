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
	"slices"
	"time"

	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// unhealthyWaitingStatus contains the status messages for a pod in waiting state
// which should be considered as unhealthy
var unhealthyWaitingStatus = []string{"CrashLoopBackOff", "ImagePullBackOff"}

// CheckPodsStatus checks the status by iterating over pods objects
func CheckPodsStatus(pods *corev1.PodList) (healthy bool, reason string, message string, transientUnhealthy bool) {
	// TODO: Need to revisit later.
	if len(pods.Items) == 0 {
		return true, "NoPodsFound", "No Pods found", false
	} else {
		for _, pod := range pods.Items {
			if podHealthy, msg, transient := isPodHealthy(&pod); !podHealthy {
				return false, "Pod" + msg, fmt.Sprintf("Pod %s is unhealthy", pod.Name), transient
			}
		}
	}
	return true, "Running", "All pods are healthy", false
}

// Check if a pod is healthy. If it's unhealthy, also tell if it's transient or not.
// The reason of transient unhealthy status is because of the logic of checking RecentRestart,
// which would not end up with another reconciliation when it reaches the time limit,
// but we have to trigger it explicitly.
func isPodHealthy(pod *corev1.Pod) (healthy bool, reason string, isTransientUnhealthy bool) {
	var lastRestartTime time.Time
	for _, c := range pod.Status.ContainerStatuses {
		if c.State.Waiting != nil && slices.Contains(unhealthyWaitingStatus, c.State.Waiting.Reason) {
			return false, c.State.Waiting.Reason, false
		}
		if c.State.Terminated != nil && c.State.Terminated.Reason == "Error" {
			return false, c.State.Terminated.Reason, false
		}
		if x := c.LastTerminationState.Terminated; x != nil && !x.FinishedAt.Time.IsZero() {
			if lastRestartTime.IsZero() || x.FinishedAt.Time.After(lastRestartTime) {
				// Only check OOM or exit with Error
				// TODO: revisit later if needed.
				if x.ExitCode == 137 || (x.ExitCode == 143 && x.Reason == "Error") {
					lastRestartTime = x.FinishedAt.Time
				}
			}
		}
	}
	// Container restart happened in the last 2 mins
	if !lastRestartTime.IsZero() && lastRestartTime.Add(2*time.Minute).After(time.Now()) {
		return false, "RecentRestart", true
	}
	return true, "", false
}

func NumOfReadyPods(pods corev1.PodList) int {
	result := 0
	for _, pod := range pods.Items {
		if IsPodReady(pod) {
			result++
		}
	}
	return result
}

func IsPodReady(pod corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.ContainerStatuses {
		if !c.Ready {
			return false
		}
	}
	return true
}

// CheckVertexStatus will calculate the status of the vertices and return the status and reason
func CheckVertexStatus(vertices *dfv1.VertexList) (healthy bool, reason string, message string) {
	for _, vertex := range vertices.Items {
		if vertex.Status.ObservedGeneration == 0 || vertex.Generation > vertex.Status.ObservedGeneration {
			return false, "Progressing", `Vertex "` + vertex.Spec.Name + `" Waiting for reconciliation`
		}
		if !vertex.Status.IsHealthy() {
			return false, "Unavailable", `Vertex "` + vertex.Spec.Name + `" is not healthy`
		}
	}
	return true, "Healthy", "All vertices are healthy"
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
				"Waiting for deployment %q rollout to finish: %d out of %d new replicas have been updated...",
				deployment.Name, deployment.Status.UpdatedReplicas, *deployment.Spec.Replicas)
		}
		if deployment.Status.Replicas > deployment.Status.UpdatedReplicas {
			return false, "Progressing", fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d old replicas are pending termination...",
				deployment.Name, deployment.Status.Replicas-deployment.Status.UpdatedReplicas)
		}
		if deployment.Status.AvailableReplicas < deployment.Status.UpdatedReplicas {
			return false, "Progressing", fmt.Sprintf(
				"Waiting for deployment %q rollout to finish: %d of %d updated replicas are available...",
				deployment.Name, deployment.Status.AvailableReplicas, deployment.Status.UpdatedReplicas)
		}
		return true, "Healthy", fmt.Sprintf("deployment %q successfully rolled out", deployment.Name)
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

// CheckStatefulSetStatus returns a message describing statefulset status, and a bool value indicating if the status is considered done.
// Borrowed at kubernetes/kubectl/rollout_status.go https://github.com/kubernetes/kubernetes/blob/cea1d4e20b4a7886d8ff65f34c6d4f95efcb4742/staging/src/k8s.io/kubectl/pkg/polymorphichelpers/rollout_status.go#L130
func CheckStatefulSetStatus(sts *appv1.StatefulSet) (done bool, reason string, message string) {
	if sts.Status.ObservedGeneration == 0 || sts.Generation > sts.Status.ObservedGeneration {
		return false, "Progressing", "Waiting for statefulset spec update to be observed..."
	}
	if sts.Status.UpdateRevision != sts.Status.CurrentRevision {
		return false, "Progressing", fmt.Sprintf("waiting for statefulset rolling update to complete %d pods at revision %s...",
			sts.Status.UpdatedReplicas, sts.Status.UpdateRevision)
	}
	if sts.Spec.Replicas != nil && sts.Status.ReadyReplicas < *sts.Spec.Replicas {
		return false, "Unavailable", fmt.Sprintf("Waiting for %d pods to be ready...\n", *sts.Spec.Replicas-sts.Status.ReadyReplicas)
	}
	if sts.Spec.UpdateStrategy.Type == appv1.RollingUpdateStatefulSetStrategyType && sts.Spec.UpdateStrategy.RollingUpdate != nil {
		if sts.Spec.Replicas != nil && sts.Spec.UpdateStrategy.RollingUpdate.Partition != nil {
			if sts.Status.UpdatedReplicas < (*sts.Spec.Replicas - *sts.Spec.UpdateStrategy.RollingUpdate.Partition) {
				return false, "Progressing", fmt.Sprintf(
					"Waiting for partitioned roll out to finish: %d out of %d new pods have been updated...\n",
					sts.Status.UpdatedReplicas, *sts.Spec.Replicas-*sts.Spec.UpdateStrategy.RollingUpdate.Partition)
			}
		}
		return true, "Healthy", fmt.Sprintf("partitioned roll out complete: %d new pods have been updated...\n",
			sts.Status.UpdatedReplicas)
	}
	return true, "Healthy", fmt.Sprintf(
		"statefulset rolling update complete %d pods at revision %s...\n",
		sts.Status.CurrentReplicas, sts.Status.CurrentRevision)
}
