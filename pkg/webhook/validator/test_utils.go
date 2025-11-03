package validator

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1/fake"
)

const testNamespace = "test-ns"

var (
	fakeNumaClient       = fake.FakeNumaflowV1alpha1{}
	testStorageClassName = "test-sc"
)

func fakeJetStreamISBSvc() *dfv1.InterStepBufferService {
	return &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      dfv1.DefaultISBSvcName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{
			JetStream: &dfv1.JetStreamBufferService{
				Version: "1.1.1",
				Persistence: &dfv1.PersistenceStrategy{
					StorageClassName: &testStorageClassName,
				},
			},
		},
	}
}

func fakePipeline() *dfv1.Pipeline {
	return &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-pl",
			Namespace:   testNamespace,
			Annotations: map[string]string{},
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

func fakeMonoVertex() *dfv1.MonoVertex {
	return &dfv1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-mvtx",
			Namespace:   testNamespace,
			Annotations: map[string]string{},
		},
		Spec: dfv1.MonoVertexSpec{
			Source: &dfv1.Source{
				UDTransformer: &dfv1.UDTransformer{
					Container: &dfv1.Container{Image: "my-transformer-image"},
				},
				UDSource: &dfv1.UDSource{
					Container: &dfv1.Container{
						Image: "my-src-image",
					},
				},
			},

			Sink: &dfv1.Sink{
				AbstractSink: dfv1.AbstractSink{
					UDSink: &dfv1.UDSink{
						Container: &dfv1.Container{
							Image: "my-sink-image",
						},
					},
				},
				Fallback: &dfv1.AbstractSink{
					UDSink: &dfv1.UDSink{
						Container: &dfv1.Container{
							Image: "my-fb-image",
						},
					},
				},
				OnSuccess: &dfv1.AbstractSink{
					UDSink: &dfv1.UDSink{
						Container: &dfv1.Container{
							Image: "my-os-image",
						},
					},
				},
			},
		},
	}
}
