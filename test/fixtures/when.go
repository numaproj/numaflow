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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	flowpkg "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
)

type When struct {
	t              *testing.T
	isbSvcClient   flowpkg.InterStepBufferServiceInterface
	pipelineClient flowpkg.PipelineInterface
	vertexClient   flowpkg.VertexInterface
	isbSvc         *dfv1.InterStepBufferService
	pipeline       *dfv1.Pipeline
	restConfig     *rest.Config
	kubeClient     kubernetes.Interface

	portForwarderStopChannels map[string]chan struct{}
	// Key: vertex label selector
	// Value: the ip of, one of the pods matching the label selector
	vertexToPodIpMapping map[string]string
}

// SendMessageTo sends msg to one of the pods in http source vertex.
func (w *When) SendMessageTo(pipelineName string, vertexName string, req HttpPostRequest) *When {
	w.t.Helper()
	if w.vertexToPodIpMapping == nil {
		w.vertexToPodIpMapping = make(map[string]string)
	}
	labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipelineName, dfv1.KeyVertexName, vertexName)
	if w.vertexToPodIpMapping[labelSelector] == "" {
		ctx := context.Background()
		podList, err := w.kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
		if err != nil {
			w.t.Fatalf("Error getting vertex pod list: %v", err)
		}
		if len(podList.Items) == 0 {
			w.t.Fatalf("No running pod found in pipeline %s, vertex: %s", pipelineName, vertexName)
		}
		w.vertexToPodIpMapping[labelSelector] = podList.Items[0].Status.PodIP
	}

	// There could be a rare corner case when a previous added pod gets replaced by a new one, making the mapping entry no longer valid.
	// Considering current e2e tests are all lightweight, we assume no such case for now.
	SendMessageTo(w.vertexToPodIpMapping[labelSelector], vertexName, req)
	return w
}

func (w *When) CreateISBSvc() *When {
	w.t.Helper()
	if w.isbSvc == nil {
		w.t.Fatal("No ISB svc to create")
	}
	w.t.Log("Creating ISB svc", w.isbSvc.Name)
	ctx := context.Background()
	i, err := w.isbSvcClient.Create(ctx, w.isbSvc, metav1.CreateOptions{})
	if err != nil {
		w.t.Fatal(err)
	} else {
		w.isbSvc = i
	}
	return w
}

func (w *When) DeleteISBSvc() *When {
	w.t.Helper()
	if w.isbSvc == nil {
		w.t.Fatal("No ISB svc to delete")
	}
	w.t.Log("Deleting ISB svc", w.isbSvc.Name)
	ctx := context.Background()
	err := w.isbSvcClient.Delete(ctx, w.isbSvc.Name, metav1.DeleteOptions{})
	if err != nil {
		w.t.Fatal(err)
	}
	return w
}

func (w *When) CreatePipelineAndWait() *When {
	w.t.Helper()
	if w.pipeline == nil {
		w.t.Fatal("No Pipeline to create")
	}
	w.t.Log("Creating Pipeline", w.pipeline.Name)
	ctx := context.Background()
	i, err := w.pipelineClient.Create(ctx, w.pipeline, metav1.CreateOptions{})
	if err != nil {
		w.t.Fatal(err)
	} else {
		w.pipeline = i
	}
	// wait
	if err := WaitForPipelineRunning(ctx, w.pipelineClient, w.pipeline.Name, defaultTimeout); err != nil {
		w.t.Fatal(err)
	}
	return w
}

func (w *When) DeletePipelineAndWait() *When {
	w.t.Helper()
	if w.pipeline == nil {
		w.t.Fatal("No Pipeline to delete")
	}
	w.t.Log("Deleting Pipeline", w.pipeline.Name)
	ctx := context.Background()
	if err := w.pipelineClient.Delete(ctx, w.pipeline.Name, metav1.DeleteOptions{}); err != nil {
		w.t.Fatal(err)
	}

	timeout := defaultTimeout
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	labelSelector := fmt.Sprintf("%s=%s", dfv1.KeyPipelineName, w.pipeline.Name)
	for {
		select {
		case <-ctx.Done():
			w.t.Fatalf("Timeout after %v waiting for pipeline pods terminating", timeout)
		default:
		}
		podList, err := w.kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			w.t.Fatalf("Error getting pipeline pods: %v", err)
		}
		if len(podList.Items) == 0 {
			return w
		}
		time.Sleep(2 * time.Second)
	}
}

func (w *When) WaitForISBSvcReady() *When {
	w.t.Helper()
	ctx := context.Background()
	if err := WaitForISBSvcReady(ctx, w.isbSvcClient, w.isbSvc.Name, defaultTimeout); err != nil {
		w.t.Fatal(err)
	}
	if err := WaitForISBSvcStatefulSetReady(ctx, w.kubeClient, Namespace, w.isbSvc.Name, 5*time.Minute); err != nil {
		w.t.Fatal(err)
	}
	return w
}

func (w *When) VertexPodPortForward(vertexName string, localPort, remotePort int) *When {
	w.t.Helper()
	labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, w.pipeline.Name, dfv1.KeyVertexName, vertexName)
	ctx := context.Background()
	podList, err := w.kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
	if err != nil {
		w.t.Fatalf("Error getting vertex pod name: %v", err)
	}
	podName := podList.Items[0].GetName()
	w.t.Logf("Vertex POD name: %s", podName)

	stopCh := make(chan struct{}, 1)
	if err = PodPortForward(w.restConfig, Namespace, podName, localPort, remotePort, stopCh); err != nil {
		w.t.Fatalf("Expected vertex pod port-forward: %v", err)
	}
	if w.portForwarderStopChannels == nil {
		w.portForwarderStopChannels = make(map[string]chan struct{})
	}
	w.portForwarderStopChannels[podName] = stopCh
	return w
}

func (w *When) DaemonPodPortForward(pipelineName string, localPort, remotePort int) *When {
	w.t.Helper()
	labelSelector := fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipelineName, dfv1.KeyComponent, dfv1.ComponentDaemon)
	ctx := context.Background()
	podList, err := w.kubeClient.CoreV1().Pods(Namespace).List(ctx, metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "status.phase=Running"})
	if err != nil {
		w.t.Fatalf("Error getting daemon pod name: %v", err)
	}
	podName := podList.Items[0].GetName()
	w.t.Logf("Daemon POD name: %s", podName)

	stopCh := make(chan struct{}, 1)
	if err = PodPortForward(w.restConfig, Namespace, podName, localPort, remotePort, stopCh); err != nil {
		w.t.Fatalf("Expected daemon pod port-forward: %v", err)
	}
	if w.portForwarderStopChannels == nil {
		w.portForwarderStopChannels = make(map[string]chan struct{})
	}
	w.portForwarderStopChannels[podName] = stopCh
	return w
}

func (w *When) TerminateAllPodPortForwards() *When {
	w.t.Helper()
	if len(w.portForwarderStopChannels) > 0 {
		for k, v := range w.portForwarderStopChannels {
			w.t.Logf("Terminating port-forward for POD %s", k)
			close(v)
		}
	}
	return w
}

func (w *When) Wait(timeout time.Duration) *When {
	w.t.Helper()
	w.t.Log("Waiting for", timeout.String())
	time.Sleep(timeout)
	w.t.Log("Done waiting")
	return w
}

func (w *When) And(block func()) *When {
	w.t.Helper()
	block()
	if w.t.Failed() {
		w.t.FailNow()
	}
	return w
}

func (w *When) Exec(name string, args []string, block func(t *testing.T, output string, err error)) *When {
	w.t.Helper()
	output, err := Exec(name, args...)
	block(w.t, output, err)
	if w.t.Failed() {
		w.t.FailNow()
	}
	return w
}

func (w *When) Given() *Given {
	return &Given{
		t:              w.t,
		isbSvcClient:   w.isbSvcClient,
		pipelineClient: w.pipelineClient,
		vertexClient:   w.vertexClient,
		isbSvc:         w.isbSvc,
		pipeline:       w.pipeline,
		restConfig:     w.restConfig,
		kubeClient:     w.kubeClient,
	}
}

func (w *When) Expect() *Expect {
	return &Expect{
		t:              w.t,
		isbSvcClient:   w.isbSvcClient,
		pipelineClient: w.pipelineClient,
		vertexClient:   w.vertexClient,
		isbSvc:         w.isbSvc,
		pipeline:       w.pipeline,
		restConfig:     w.restConfig,
		kubeClient:     w.kubeClient,
	}
}
