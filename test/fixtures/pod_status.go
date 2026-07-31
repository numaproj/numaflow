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

package fixtures

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	flowpkg "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
)

// PodRuntimeSnapshot captures pod identity and numa container restart state.
type PodRuntimeSnapshot struct {
	PodName          string
	UID              types.UID
	NumaRestartCount int32
}

func numaRestartCount(pod corev1.Pod) (int32, bool) {
	for _, c := range pod.Status.ContainerStatuses {
		if c.Name == dfv1.CtrMain {
			return c.RestartCount, true
		}
	}
	return 0, false
}

func snapshotFromPod(pod corev1.Pod) (PodRuntimeSnapshot, error) {
	restarts, ok := numaRestartCount(pod)
	if !ok {
		return PodRuntimeSnapshot{}, fmt.Errorf("numa container status not found for pod %q", pod.Name)
	}
	return PodRuntimeSnapshot{
		PodName:          pod.Name,
		UID:              pod.UID,
		NumaRestartCount: restarts,
	}, nil
}

func vertexPodLabelSelector(pipelineName, vertexName string) string {
	return fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipelineName, dfv1.KeyVertexName, vertexName)
}

func monoVertexPodLabelSelector(monoVertexName string) string {
	return fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyMonoVertexName, monoVertexName, dfv1.KeyComponent, dfv1.ComponentMonoVertex)
}

// getPodSnapshotByName re-reads the exact pod captured in a baseline snapshot. Looking the pod up by
// name (instead of picking the first one matching the labels) makes the comparison meaningful when the
// pod has been replaced or the vertex has more than one replica.
func getPodSnapshotByName(kubeClient kubernetes.Interface, namespace, podName string) (PodRuntimeSnapshot, error) {
	pod, err := kubeClient.CoreV1().Pods(namespace).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		if apierr.IsNotFound(err) {
			return PodRuntimeSnapshot{}, fmt.Errorf("pod %q no longer exists, it was replaced", podName)
		}
		return PodRuntimeSnapshot{}, fmt.Errorf("error getting pod %q: %w", podName, err)
	}
	return snapshotFromPod(*pod)
}

func getRunningVertexPodSnapshot(kubeClient kubernetes.Interface, namespace, pipelineName, vertexName string) (PodRuntimeSnapshot, error) {
	podList, err := kubeClient.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: vertexPodLabelSelector(pipelineName, vertexName),
		FieldSelector: "status.phase=Running",
	})
	if err != nil {
		return PodRuntimeSnapshot{}, fmt.Errorf("error getting vertex pod list: %w", err)
	}
	if len(podList.Items) == 0 {
		return PodRuntimeSnapshot{}, fmt.Errorf("no running pod found for vertex %q", vertexName)
	}
	return snapshotFromPod(podList.Items[0])
}

func getRunningMonoVertexPodSnapshot(kubeClient kubernetes.Interface, namespace, monoVertexName string) (PodRuntimeSnapshot, error) {
	podList, err := kubeClient.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: monoVertexPodLabelSelector(monoVertexName),
		FieldSelector: "status.phase=Running",
	})
	if err != nil {
		return PodRuntimeSnapshot{}, fmt.Errorf("error getting monovertex pod list: %w", err)
	}
	if len(podList.Items) == 0 {
		return PodRuntimeSnapshot{}, fmt.Errorf("no running pod found for monovertex %q", monoVertexName)
	}
	return snapshotFromPod(podList.Items[0])
}

// WaitForVertexPodRuntimeSnapshot waits until the vertex pod is running and reports numa container status.
// Unlike WaitForVertexPodRunning, it does not require every container in the pod to be ready.
func WaitForVertexPodRuntimeSnapshot(kubeClient kubernetes.Interface, vertexClient flowpkg.VertexInterface, namespace, pipelineName, vertexName string, timeout time.Duration) (PodRuntimeSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return PodRuntimeSnapshot{}, fmt.Errorf("timeout after %v waiting for vertex pod runtime snapshot", timeout)
		default:
		}
		if _, err := vertexClient.Get(ctx, pipelineName+"-"+vertexName, metav1.GetOptions{}); err != nil {
			return PodRuntimeSnapshot{}, fmt.Errorf("error getting vertex: %w", err)
		}
		snapshot, err := getRunningVertexPodSnapshot(kubeClient, namespace, pipelineName, vertexName)
		if err == nil {
			return snapshot, nil
		}
		time.Sleep(2 * time.Second)
	}
}

// WaitForMonoVertexPodRuntimeSnapshot waits until the MonoVertex pod is running and reports numa container status.
func WaitForMonoVertexPodRuntimeSnapshot(kubeClient kubernetes.Interface, monoVertexClient flowpkg.MonoVertexInterface, namespace, monoVertexName string, timeout time.Duration) (PodRuntimeSnapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			return PodRuntimeSnapshot{}, fmt.Errorf("timeout after %v waiting for monovertex pod runtime snapshot", timeout)
		default:
		}
		if _, err := monoVertexClient.Get(ctx, monoVertexName, metav1.GetOptions{}); err != nil {
			return PodRuntimeSnapshot{}, fmt.Errorf("error getting monovertex: %w", err)
		}
		snapshot, err := getRunningMonoVertexPodSnapshot(kubeClient, namespace, monoVertexName)
		if err == nil {
			return snapshot, nil
		}
		time.Sleep(2 * time.Second)
	}
}
