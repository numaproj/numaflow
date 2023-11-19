package validator

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1/fake"
)

const testNamespace = "test-ns"

var fakeNumaClient = fake.FakeNumaflowV1alpha1{}

func fakeRedisISBSvc() *dfv1.InterStepBufferService {
	return &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      dfv1.DefaultISBSvcName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{
			Redis: &dfv1.RedisBufferService{
				Native: &dfv1.NativeRedis{
					Version: "6.2.6",
				},
			},
		},
	}
}

func fakeJetStreamISBSvc() *dfv1.InterStepBufferService {
	return &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      dfv1.DefaultISBSvcName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{
			JetStream: &dfv1.JetStreamBufferService{
				Version: "1.1.1",
			},
		},
	}
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
							Builtin: &dfv1.Transformer{Name: "filter"},
						}},
				},
				{
					Name: "map",
					UDF: &dfv1.UDF{
						Builtin: &dfv1.Function{Name: "cat"},
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
