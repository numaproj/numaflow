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
