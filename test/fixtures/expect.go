package fixtures

import (
	"context"
	"fmt"
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

func (t *Expect) ISBSvcDeleted(timeout time.Duration) *Expect {
	t.t.Helper()
	ctx := context.Background()
	_, err := t.isbSvcClient.Get(ctx, t.isbSvc.Name, metav1.GetOptions{})
	if err == nil || !apierr.IsNotFound(err) {
		t.t.Fatalf("expected ISB svc to be deleted: %v", err)
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
			t.t.Fatalf("failed to check if ISB svc pods have been deleted: %v", err)
		}
		if len(podList.Items) == 0 {
			return t
		}
		select {
		case <-timeoutCh:
			t.t.Fatalf("timeout after %v waiting for ISB svc to be deleted", timeout)
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
			t.t.Fatalf("expected vertex %q existing: %v", v.Name, err)
		}
	}
	// check pods running
	timeout := 2 * time.Minute
	for _, v := range t.pipeline.Spec.Vertices {
		if err := WaitForVertexPodRunning(t.kubeClient, Namespace, t.pipeline.Name, v.Name, timeout); err != nil {
			t.t.Fatalf("expected vertex %q pod running: %v", v.Name, err)
		}
	}
	return t
}

func (t *Expect) VertexPodLogContains(vertexName, containerName, regex string) *Expect {
	t.t.Helper()
	ctx := context.Background()
	contains, err := VertexPodLogContains(ctx, t.kubeClient, Namespace, t.pipeline.Name, vertexName, containerName, regex, defaultTimeout)
	if err != nil {
		t.t.Fatalf("expected vertex pod logs: %v", err)
	}
	if !contains {
		t.t.Fatalf("expected vertex pod log contains %q", regex)
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
