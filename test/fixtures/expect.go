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
	"k8s.io/apimachinery/pkg/util/wait"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	flowpkg "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Expect struct {
	t              *testing.T
	isbSvcClient   flowpkg.InterStepBufferServiceInterface
	pipelineClient flowpkg.PipelineInterface
	vertexClient   flowpkg.VertexInterface
	isbSvc         *dfv1.InterStepBufferService
	pipeline       *dfv1.Pipeline
	restConfig     *rest.Config
	kubeClient     kubernetes.Interface
}

func (t *Expect) RedisContains(sinkName string, targetRegex string, opts ...RedisCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	contains := RedisContains(ctx, sinkName, targetRegex, opts...)
	if !contains {
		t.t.Fatalf("Redis doesn't contain enough data matching target regex %s", targetRegex)
	}
	return t
}

func (t *Expect) RedisNotContains(sinkName string, regex string) *Expect {
	t.t.Helper()
	ctx := context.Background()
	notContains := RedisNotContains(ctx, sinkName, regex)
	if !notContains {
		t.t.Fatalf("Sink %s contains regex %s", sinkName, regex)
	}
	return t
}

func (t *Expect) ISBSvcDeleted(timeout time.Duration) *Expect {
	t.t.Helper()
	ctx := context.Background()
	_, err := t.isbSvcClient.Get(ctx, t.isbSvc.Name, metav1.GetOptions{})
	if err == nil || !apierr.IsNotFound(err) {
		t.t.Fatalf("Expected ISB svc to be deleted: %v", err)
	}

	labelSelector := fmt.Sprintf("%s=isbsvc-controller,%s=%s", dfv1.KeyManagedBy, dfv1.KeyISBSvcName, ISBSvcName)
	opts := metav1.ListOptions{LabelSelector: labelSelector}
	timeoutCh := make(chan bool, 1)
	go func() {
		time.Sleep(timeout)
		timeoutCh <- true
	}()
	for {
		podList, err := t.kubeClient.CoreV1().Pods(Namespace).List(ctx, opts)
		if err != nil && !apierr.IsNotFound(err) {
			t.t.Fatalf("Failed to check if ISB svc pods have been deleted: %v", err)
		}
		if len(podList.Items) == 0 {
			return t
		}
		select {
		case <-timeoutCh:
			t.t.Fatalf("Timeout after %v waiting for ISB svc to be deleted", timeout)
		default:
		}
	}
}

func (t *Expect) VertexPodsRunning() *Expect {
	t.t.Helper()
	ctx := context.Background()
	for _, v := range t.pipeline.Spec.Vertices {
		_, err := t.vertexClient.Get(ctx, t.pipeline.Name+"-"+v.Name, metav1.GetOptions{})
		if err != nil {
			t.t.Fatalf("Expected vertex %q existing: %v", v.Name, err)
		}
	}
	// check pods running
	timeout := 2 * time.Minute
	for _, v := range t.pipeline.Spec.Vertices {
		if err := WaitForVertexPodRunning(t.kubeClient, t.vertexClient, Namespace, t.pipeline.Name, v.Name, timeout); err != nil {
			t.t.Fatalf("Expected vertex %q pod running: %v", v.Name, err)
		}
	}
	return t
}

func (t *Expect) VertexPodLogContains(vertexName, regex string, opts ...PodLogCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	contains, err := VertexPodLogContains(ctx, t.kubeClient, Namespace, t.pipeline.Name, vertexName, regex, opts...)
	if err != nil {
		t.t.Fatalf("Failed to check vertex %q pod logs: %v", vertexName, err)
	}
	if !contains {
		t.t.Fatalf("Expected vertex %q pod log contains %q", vertexName, regex)
	}
	return t
}

func (t *Expect) HttpVertexReadyForPost(pipelineName, vertexName string) *Expect {
	t.t.Helper()
	ctx := context.Background()

	// Posting right after vertex creation sometimes gets the "dial tcp: connect: connection refused" error.
	// Adding retry to mitigate such issue.
	// 3 attempts with 2 second fixed wait time are tested sufficient for it.
	var retryBackOff = wait.Backoff{
		Factor:   1,
		Jitter:   0,
		Steps:    3,
		Duration: time.Second * 2,
	}

	err := wait.ExponentialBackoffWithContext(ctx, retryBackOff, func() (done bool, err error) {
		err = SendMessageTo(pipelineName, vertexName, []byte("test"))
		if err == nil {
			return true, nil
		}

		fmt.Printf("Got error %v, retrying.\n", err)
		return false, nil
	})

	if err != nil {
		t.t.Fatalf("Http vertex %s is not ready to serve post request.", vertexName)
	}

	return t
}

func (t *Expect) VertexPodLogNotContains(vertexName, regex string, opts ...PodLogCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	yes, err := VertexPodLogNotContains(ctx, t.kubeClient, Namespace, t.pipeline.Name, vertexName, regex, opts...)
	if err != nil {
		t.t.Fatalf("Failed to check vertex pod logs: %v", err)
	}
	if !yes {
		t.t.Fatalf("Not expected vertex %q pod log contains %q", vertexName, regex)
	}
	return t
}

func (t *Expect) DaemonPodsRunning() *Expect {
	t.t.Helper()
	timeout := 2 * time.Minute
	if err := WaitForDaemonPodsRunning(t.kubeClient, Namespace, t.pipeline.Name, timeout); err != nil {
		t.t.Fatalf("Expected daemon pods of pipeline %q running: %v", t.pipeline.Name, err)
	}
	return t
}

func (t *Expect) DaemonPodLogContains(pipelineName, regex string, opts ...PodLogCheckOption) *Expect {
	t.t.Helper()
	ctx := context.Background()
	contains, err := DaemonPodLogContains(ctx, t.kubeClient, Namespace, t.pipeline.Name, regex, opts...)
	if err != nil {
		t.t.Fatalf("Failed to check daemon pod logs: %v", err)
	}
	if !contains {
		t.t.Fatalf("Expected daemon pod log contains %q", regex)
	}
	return t
}

func (t *Expect) When() *When {
	return &When{
		t:              t.t,
		isbSvcClient:   t.isbSvcClient,
		pipelineClient: t.pipelineClient,
		vertexClient:   t.vertexClient,
		isbSvc:         t.isbSvc,
		pipeline:       t.pipeline,
		restConfig:     t.restConfig,
		kubeClient:     t.kubeClient,
	}
}
