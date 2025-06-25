package v1

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1/fake"
)

const testNamespace = "test-ns"

var (
	fakeNumaClient    = fake.FakeNumaflowV1alpha1{}
	fakeKubeClient    = fakeclientset.NewSimpleClientset()
	testContainerName = "test-container"
)

// getContainerStatus returns a container status with the given phase.
func getContainerStatus(phase string) corev1.ContainerStatus {
	switch phase {
	case "running":
		return corev1.ContainerStatus{
			Name:  testContainerName,
			Ready: true,
			State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.Time{Time: time.Now()},
				},
			},
		}
	case "waiting":
		return corev1.ContainerStatus{
			Name:  testContainerName,
			Ready: false,
			State: corev1.ContainerState{
				Waiting: &corev1.ContainerStateWaiting{
					Reason:  "test-reason",
					Message: "test-message",
				},
			},
		}
	}
	return corev1.ContainerStatus{}
}

// fakePod returns a fake pod with the given pipeline name, vertex name, namespace and phase.
func fakePod(pipelineName string, vertexName string, namespace string, phase string) *corev1.Pod {
	containerStatus := getContainerStatus(phase)
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: namespace,
			Labels: map[string]string{
				dfv1.KeyPipelineName: pipelineName,
				dfv1.KeyVertexName:   vertexName,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: testContainerName,
				},
			},
		},
		Status: corev1.PodStatus{
			Phase:             corev1.PodPhase("Running"),
			ContainerStatuses: []corev1.ContainerStatus{containerStatus},
		},
	}
	return pod
}

func fakePipeline() *dfv1.Pipeline {
	return &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pl",
			Namespace: testNamespace,
		},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name: "input",
					Source: &dfv1.Source{
						UDTransformer: &dfv1.UDTransformer{
							Container: &dfv1.Container{Image: "test-image"},
						}},
				},
				{
					Name: "map",
					UDF: &dfv1.UDF{
						Container: &dfv1.Container{
							Image: "my-image",
						},
					},
				},
				{
					Name: "reduce",
					UDF: &dfv1.UDF{
						Container: &dfv1.Container{
							Image: "test-image",
						},
						GroupBy: &dfv1.GroupBy{
							Window: dfv1.Window{
								Fixed: &dfv1.FixedWindow{Length: &metav1.Duration{
									Duration: 60 * time.Second,
								}},
							},
							Keyed: true,
							Storage: &dfv1.PBQStorage{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "map"},
				{From: "map", To: "reduce"},
				{From: "reduce", To: "output"},
			},
		},
	}
}

// fakeVertex returns a fake vertex with the given name and phase.
func fakeVertex(name string, phase dfv1.VertexPhase) *dfv1.Vertex {
	v := &dfv1.Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Status: dfv1.VertexStatus{
			Phase:           phase,
			Replicas:        1,
			DesiredReplicas: 1,
		},
		Spec: dfv1.VertexSpec{
			Replicas: ptr.To[int32](1),
		},
	}
	return v
}
