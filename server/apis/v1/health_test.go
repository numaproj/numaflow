package v1

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	appv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

const (
	RunningPod = "running"
	WaitingPod = "waiting"
)

// Client is the struct to hold the Kubernetes Clientset
type Client struct {
	Clientset kubernetes.Interface
}

func init() {
	_ = dfv1.AddToScheme(scheme.Scheme)
	_ = appv1.AddToScheme(scheme.Scheme)
	//_ = corev1.AddToScheme(scheme.Scheme)
}

func (c Client) CreatePod(pod *v1.Pod) (*v1.Pod, error) {
	_, err := c.Clientset.CoreV1().Pods(pod.Namespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		fmt.Printf("Error occured while creating pod %s: %s", pod.Name, err.Error())
		return nil, err
	}

	fmt.Printf("Pod %s is succesfully created", pod.Name)
	return pod, nil
}

func createPod(phase string) {
	client := Client{
		Clientset: fakeKubeClient,
	}
	pod := fakePod("test-pl", "test-vertex", testNamespace, phase)
	_, err := client.CreatePod(pod)
	if err != nil {
		fmt.Print(err.Error())
	}
	pod, err = client.Clientset.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{})
	if err != nil {
		fmt.Printf("Error occured while getting pod %s: %s", pod.Name, err.Error())
	}
}

func removePod() {
	client := Client{
		Clientset: fakeKubeClient,
	}
	pod := fakePod("test-pl", "test-vertex", testNamespace, "running")
	err := client.Clientset.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{})
	if err != nil {
		fmt.Printf("Error occured while deleting pod %s: %s", pod.Name, err.Error())
	}
}

// fakePipeline returns a fake pipeline for testing

// TestIsVertexHealthy verifies the functionality of the vertex level health check
// It creates a fake pipeline and checks the health of each vertex
// We test multiple scenarios:
// 1. All vertices are healthy
// 2. One or more vertices are unhealthy
// 3. The number of replicas running in the vertex is less than the expected number of replicas
// 4. Vertex is not in running state
func TestIsVertexHealthy(t *testing.T) {
	// Test vertex is in running phase and the pods are in running state
	t.Run("test all pods are healthy", func(t *testing.T) {
		pipeline := fakePipeline()
		vertexName := "test-vertex"
		vertex := fakeVertex(vertexName, dfv1.VertexPhaseRunning)

		// Create fake handler
		h := &handler{
			kubeClient:     fakeKubeClient,
			numaflowClient: &fakeNumaClient,
		}

		// Create fake pod in running state
		createPod(RunningPod)
		defer removePod()
		healthy, _, err := isVertexHealthy(h, testNamespace, pipeline.GetName(), vertex, vertexName)
		if err != nil {
			return
		}
		assert.True(t, healthy)
	})

	// Test vertex is in running phase and the pods are in waiting state
	t.Run("test pod not running", func(t *testing.T) {
		pipeline := fakePipeline()
		vertexName := "test-vertex"
		vertex := fakeVertex(vertexName, dfv1.VertexPhaseRunning)

		// Create fake handler
		h := &handler{
			kubeClient:     fakeKubeClient,
			numaflowClient: &fakeNumaClient,
		}
		createPod(WaitingPod)
		defer removePod()
		healthy, r, err := isVertexHealthy(h, testNamespace, pipeline.GetName(), vertex, vertexName)
		if err != nil {
			return
		}
		assert.False(t, healthy)
		assert.Equal(t, "V3", r.Code)
	})

	// Test vertex is not in running state
	t.Run("test vertex not in running phase", func(t *testing.T) {
		pipeline := fakePipeline()
		vertexName := "test-vertex"
		// Create a fake vertex in failed state
		vertex := fakeVertex(vertexName, dfv1.VertexPhaseFailed)

		// Create fake handler
		h := &handler{
			kubeClient:     fakeKubeClient,
			numaflowClient: &fakeNumaClient,
		}
		healthy, r, err := isVertexHealthy(h, testNamespace, pipeline.GetName(), vertex, vertexName)
		if err != nil {
			return
		}
		assert.False(t, healthy)
		// Refer: pkg/shared/health-status-code
		assert.Equal(t, "V2", r.Code)
	})

	// Test vertex replica count is not equal to the desired replica count
	t.Run("test vertex replica count not equal to desired replica count", func(t *testing.T) {
		pipeline := fakePipeline()
		vertexName := "test-vertex"
		// Create a fake vertex in failed state
		vertex := fakeVertex(vertexName, dfv1.VertexPhaseFailed)
		// Update the desired replica count to 2
		vertex.Status.DesiredReplicas = 2

		// Create fake handler
		h := &handler{
			kubeClient:     fakeKubeClient,
			numaflowClient: &fakeNumaClient,
		}
		healthy, r, err := isVertexHealthy(h, testNamespace, pipeline.GetName(), vertex, vertexName)
		if err != nil {
			return
		}
		assert.False(t, healthy)
		// Refer: pkg/shared/health-status-code
		assert.Equal(t, "V9", r.Code)
	})
}
