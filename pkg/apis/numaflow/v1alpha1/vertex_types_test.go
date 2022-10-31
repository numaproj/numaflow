package v1alpha1

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

const (
	testNamespace      = "test-ns"
	testVertexSpecName = "vtx"
	testPipelineName   = "test-pl"
	testVertexName     = testPipelineName + "-" + testVertexSpecName
	testFlowImage      = "test-d-iamge"
)

var (
	testReplicas  = int32(1)
	testSrcVertex = &Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testVertexName,
		},
		Spec: VertexSpec{
			Replicas:     &testReplicas,
			PipelineName: testPipelineName,
			AbstractVertex: AbstractVertex{
				Name:   testVertexSpecName,
				Source: &Source{},
			},
			ToEdges: []Edge{{From: testVertexSpecName, To: "output"}},
		},
	}

	testSinkVertex = &Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testVertexName,
		},
		Spec: VertexSpec{
			Replicas:     &testReplicas,
			PipelineName: testPipelineName,
			AbstractVertex: AbstractVertex{
				Name: testVertexSpecName,
				Sink: &Sink{},
			},
			FromEdges: []Edge{{From: "input", To: testVertexSpecName}},
		},
	}

	testVertex = &Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testVertexName,
		},
		Spec: VertexSpec{
			Replicas:     &testReplicas,
			PipelineName: testPipelineName,
			AbstractVertex: AbstractVertex{
				Name: testVertexSpecName,
			},
			FromEdges: []Edge{{From: "input", To: testVertexSpecName}},
			ToEdges:   []Edge{{From: testVertexSpecName, To: "output"}},
		},
	}
)

func TestGetFromBuffers(t *testing.T) {
	f := testVertex.GetFromBuffers()
	assert.Equal(t, 1, len(f))
	assert.Equal(t, f[0].Name, fmt.Sprintf("%s-%s-%s-%s", testVertex.Namespace, testVertex.Spec.PipelineName, "input", testVertex.Spec.Name))
}

func TestGetFromBuffersSource(t *testing.T) {
	f := testSrcVertex.GetFromBuffers()
	assert.Equal(t, 1, len(f))
	assert.Equal(t, f[0].Name, fmt.Sprintf("%s-%s-%s_SOURCE", testVertex.Namespace, testVertex.Spec.PipelineName, testVertex.Spec.Name))
}

func TestGetToBuffers(t *testing.T) {
	f := testVertex.GetToBuffers()
	assert.Equal(t, 1, len(f))
	assert.Contains(t, f[0].Name, fmt.Sprintf("%s-%s-%s-%s", testVertex.Namespace, testVertex.Spec.PipelineName, testVertex.Spec.Name, "output"))
}

func TestGetToBuffersSink(t *testing.T) {
	f := testSinkVertex.GetToBuffers()
	assert.Equal(t, f[0].Name, fmt.Sprintf("%s-%s-%s_SINK", testVertex.Namespace, testVertex.Spec.PipelineName, testVertex.Spec.Name))
}

func TestWithoutReplicas(t *testing.T) {
	s := &VertexSpec{
		Replicas: pointer.Int32(3),
	}
	assert.Equal(t, int32(0), *s.WithOutReplicas().Replicas)
}

func TestGetVertexReplicas(t *testing.T) {
	v := Vertex{
		Spec: VertexSpec{
			AbstractVertex: AbstractVertex{
				Name: "b",
			},
		},
	}
	assert.Equal(t, 1, v.GetReplicas())
	v.Spec.Replicas = pointer.Int32(3)
	assert.Equal(t, 3, v.GetReplicas())
	v.Spec.Replicas = pointer.Int32(0)
	assert.Equal(t, 0, v.GetReplicas())
	v.Spec.UDF = &UDF{
		GroupBy: &GroupBy{},
	}
	v.Spec.FromEdges = []Edge{
		{From: "a", To: "b", Parallelism: pointer.Int32(5)},
	}
	assert.Equal(t, 0, v.GetReplicas())
	v.Spec.Replicas = pointer.Int32(1000)
	assert.Equal(t, 5, v.GetReplicas())
}

func TestGetHeadlessSvcSpec(t *testing.T) {
	s := testVertex.getServiceObj(testVertex.GetHeadlessServiceName(), true, VertexMetricsPort, VertexMetricsPortName)
	assert.Equal(t, s.Name, testVertex.GetHeadlessServiceName())
	assert.Equal(t, s.Namespace, testVertex.Namespace)
	assert.Equal(t, 1, len(s.Spec.Ports))
	assert.Equal(t, VertexMetricsPort, int(s.Spec.Ports[0].Port))
	assert.Equal(t, "None", s.Spec.ClusterIP)
}

func TestGetServiceObjs(t *testing.T) {
	s := testVertex.GetServiceObjs()
	assert.Equal(t, 1, len(s))

	v := testVertex.DeepCopy()
	v.Spec.UDF = nil
	v.Spec.Source = &Source{
		HTTP: &HTTPSource{},
	}
	s = v.GetServiceObjs()
	assert.Equal(t, 1, len(s))
	assert.Equal(t, s[0].Name, v.GetHeadlessServiceName())
	assert.Equal(t, 1, len(s[0].Spec.Ports))
	assert.Equal(t, VertexMetricsPort, int(s[0].Spec.Ports[0].Port))
	assert.Equal(t, "None", s[0].Spec.ClusterIP)

	v.Spec.Source.HTTP.Service = true
	s = v.GetServiceObjs()
	assert.Equal(t, 2, len(s))
	assert.Equal(t, s[1].Name, v.Name)
	assert.Equal(t, 1, len(s[1].Spec.Ports))
	assert.Equal(t, VertexHTTPSPort, int(s[1].Spec.Ports[0].Port))
}

func TestGetHeadlessServiceName(t *testing.T) {
	n := testVertex.GetHeadlessServiceName()
	assert.True(t, strings.HasSuffix(n, "-headless"))
}

func TestGetPodSpec(t *testing.T) {
	req := GetVertexPodSpecReq{
		ISBSvcType: ISBSvcTypeRedis,
		Image:      testFlowImage,
		PullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			{Name: "test-env", Value: "test-val"},
		},
	}
	t.Run("test source", func(t *testing.T) {
		testObj := testVertex.DeepCopy()
		testObj.Spec.Source = &Source{}
		s, err := testObj.GetPodSpec(req)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(s.Containers))
		assert.Equal(t, CtrMain, s.Containers[0].Name)
		assert.Equal(t, testFlowImage, s.Containers[0].Image)
		assert.Equal(t, corev1.PullIfNotPresent, s.Containers[0].ImagePullPolicy)
		envNames := []string{}
		for _, e := range s.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, "test-env")
		assert.Contains(t, envNames, EnvNamespace)
		assert.Contains(t, envNames, EnvPod)
		assert.Contains(t, envNames, EnvPipelineName)
		assert.Contains(t, envNames, EnvVertexName)
		assert.Contains(t, envNames, EnvVertexObject)
		assert.Contains(t, envNames, EnvReplica)
		assert.Contains(t, s.Containers[0].Args, "processor")
		assert.Contains(t, s.Containers[0].Args, "--type="+string(VertexTypeSource))
		assert.Equal(t, 1, len(s.InitContainers))
		assert.Equal(t, CtrInit, s.InitContainers[0].Name)
	})

	t.Run("test sink", func(t *testing.T) {
		testObj := testVertex.DeepCopy()
		testObj.Spec.Sink = &Sink{}
		s, err := testObj.GetPodSpec(req)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(s.Containers))
		assert.Equal(t, CtrMain, s.Containers[0].Name)
		assert.Equal(t, testFlowImage, s.Containers[0].Image)
		assert.Equal(t, corev1.PullIfNotPresent, s.Containers[0].ImagePullPolicy)
		assert.NotNil(t, s.Containers[0].ReadinessProbe)
		assert.NotNil(t, s.Containers[0].ReadinessProbe.HTTPGet)
		assert.Equal(t, corev1.URISchemeHTTPS, s.Containers[0].ReadinessProbe.HTTPGet.Scheme)
		assert.Equal(t, VertexMetricsPort, s.Containers[0].ReadinessProbe.HTTPGet.Port.IntValue())
		assert.NotNil(t, s.Containers[0].LivenessProbe)
		assert.NotNil(t, s.Containers[0].LivenessProbe.HTTPGet)
		assert.Equal(t, corev1.URISchemeHTTPS, s.Containers[0].LivenessProbe.HTTPGet.Scheme)
		assert.Equal(t, VertexMetricsPort, s.Containers[0].LivenessProbe.HTTPGet.Port.IntValue())
		envNames := []string{}
		for _, e := range s.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, "test-env")
		assert.Contains(t, envNames, EnvNamespace)
		assert.Contains(t, envNames, EnvPod)
		assert.Contains(t, envNames, EnvPipelineName)
		assert.Contains(t, envNames, EnvVertexName)
		assert.Contains(t, envNames, EnvVertexObject)
		assert.Contains(t, envNames, EnvReplica)
		assert.Contains(t, s.Containers[0].Args, "processor")
		assert.Contains(t, s.Containers[0].Args, "--type="+string(VertexTypeSink))
		assert.Equal(t, 1, len(s.InitContainers))
		assert.Equal(t, CtrInit, s.InitContainers[0].Name)
	})

	t.Run("test user defind sink", func(t *testing.T) {
		testObj := testVertex.DeepCopy()
		testObj.Spec.Sink = &Sink{
			UDSink: &UDSink{
				Container: Container{
					Image:   "image",
					Command: []string{"cmd"},
					Args:    []string{"arg0"},
				},
			},
		}
		s, err := testObj.GetPodSpec(req)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(s.Containers))
		assert.Equal(t, "image", s.Containers[1].Image)
		assert.Equal(t, 1, len(s.Containers[1].Command))
		assert.Equal(t, "cmd", s.Containers[1].Command[0])
		assert.Equal(t, 1, len(s.Containers[1].Args))
		assert.Equal(t, "arg0", s.Containers[1].Args[0])
	})

	t.Run("test udf", func(t *testing.T) {
		testObj := testVertex.DeepCopy()
		testObj.Spec.UDF = &UDF{
			Builtin: &Function{
				Name: "cat",
			},
		}
		s, err := testObj.GetPodSpec(req)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(s.Containers))
		assert.Equal(t, CtrMain, s.Containers[0].Name)
		assert.Equal(t, CtrUdf, s.Containers[1].Name)
		assert.Equal(t, testFlowImage, s.Containers[0].Image)
		assert.Equal(t, corev1.PullIfNotPresent, s.Containers[0].ImagePullPolicy)
		envNames := []string{}
		for _, e := range s.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, "test-env")
		assert.Contains(t, envNames, EnvNamespace)
		assert.Contains(t, envNames, EnvPod)
		assert.Contains(t, envNames, EnvPipelineName)
		assert.Contains(t, envNames, EnvVertexName)
		assert.Contains(t, envNames, EnvVertexObject)
		assert.Contains(t, envNames, EnvReplica)
		assert.Contains(t, s.Containers[0].Args, "processor")
		assert.Contains(t, s.Containers[0].Args, "--type="+string(VertexTypeMapUDF))
		assert.Equal(t, 1, len(s.InitContainers))
		assert.Equal(t, CtrInit, s.InitContainers[0].Name)
	})
}

func Test_getType(t *testing.T) {
	t.Run("test get source", func(t *testing.T) {
		testObj := testVertex.DeepCopy()
		testObj.Spec.Source = &Source{}
		_, ok := testObj.Spec.getType().(*Source)
		assert.True(t, ok)
	})

	t.Run("test get sink", func(t *testing.T) {
		testObj := testVertex.DeepCopy()
		testObj.Spec.Sink = &Sink{}
		_, ok := testObj.Spec.getType().(*Sink)
		assert.True(t, ok)
	})

	t.Run("test get udf", func(t *testing.T) {
		testObj := testVertex.DeepCopy()
		testObj.Spec.UDF = &UDF{}
		_, ok := testObj.Spec.getType().(*UDF)
		assert.True(t, ok)
	})
}

func TestVertexMarkPhase(t *testing.T) {
	s := VertexStatus{}
	s.MarkPhase(VertexPhasePending, "reason", "message")
	assert.Equal(t, VertexPhasePending, s.Phase)
	assert.Equal(t, "reason", s.Reason)
	assert.Equal(t, "message", s.Message)
}

func TestVertexMarkPhaseRunning(t *testing.T) {
	s := VertexStatus{}
	s.MarkPhaseRunning()
	assert.Equal(t, VertexPhaseRunning, s.Phase)
}

func TestVertexMarkPhaseFailed(t *testing.T) {
	s := VertexStatus{}
	s.MarkPhaseFailed("reason", "message")
	assert.Equal(t, VertexPhaseFailed, s.Phase)
	assert.Equal(t, "reason", s.Reason)
	assert.Equal(t, "message", s.Message)
}

func Test_VertexIsSource(t *testing.T) {
	o := testVertex.DeepCopy()
	o.Spec.Source = &Source{}
	assert.True(t, o.IsASource())
}

func Test_VertexIsSink(t *testing.T) {
	o := testVertex.DeepCopy()
	o.Spec.Sink = &Sink{}
	assert.True(t, o.IsASink())
}

func Test_VertexGetInitContainers(t *testing.T) {
	req := GetVertexPodSpecReq{
		ISBSvcType: ISBSvcTypeRedis,
		Image:      testFlowImage,
		PullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			{Name: "test-env", Value: "test-val"},
		},
	}
	o := testVertex.DeepCopy()
	o.Spec.Sink = &Sink{}
	o.Spec.InitContainers = []corev1.Container{
		{Name: "my-test-init", Image: "my-test-init-image"},
	}
	s := o.getInitContainers(req)
	assert.Len(t, s, 2)
	assert.Equal(t, CtrInit, s[0].Name)
	assert.Equal(t, "my-test-init", s[1].Name)
	assert.Equal(t, "my-test-init-image", s[1].Image)
	a := []string{}
	for _, env := range s[0].Env {
		a = append(a, env.Name)
	}
	for _, env := range s[0].Env {
		assert.Contains(t, a, env.Name)
	}
}

func TestGenerateEdgeBufferName(t *testing.T) {
	assert.Equal(t, []string{"a-b-c-d"}, GenerateEdgeBufferNames("a", "b", Edge{From: "c", To: "d"}))
}

func TestScalable(t *testing.T) {
	v := Vertex{}
	v.Spec.Scale.Disabled = true
	assert.False(t, v.Scalable())
	v.Spec.Scale.Disabled = false
	v.Spec.Sink = &Sink{}
	assert.True(t, v.Scalable())
	v.Spec.Sink = nil
	v.Spec.UDF = &UDF{}
	assert.True(t, v.Scalable())
	v.Spec.UDF = nil
	v.Spec.Source = &Source{
		HTTP: &HTTPSource{},
	}
	assert.False(t, v.Scalable())
	v.Spec.Source = &Source{
		Kafka: &KafkaSource{},
	}
	assert.True(t, v.Scalable())
}

func Test_Scale_Parameters(t *testing.T) {
	s := Scale{}
	assert.Equal(t, int32(0), s.GetMinReplicas())
	assert.Equal(t, int32(DefaultMaxReplicas), s.GetMaxReplicas())
	assert.Equal(t, DefaultCooldownSeconds, s.GetCooldownSeconds())
	assert.Equal(t, DefaultLookbackSeconds, s.GetLookbackSeconds())
	assert.Equal(t, DefaultReplicasPerScale, s.GetReplicasPerScale())
	assert.Equal(t, DefaultTargetBufferUsage, s.GetTargetBufferUsage())
	assert.Equal(t, DefaultTargetProcessingSeconds, s.GetTargetProcessingSeconds())
	assert.Equal(t, DefaultZeroReplicaSleepSeconds, s.GetZeroReplicaSleepSeconds())
	cds := uint32(100)
	lbs := uint32(101)
	rps := uint32(3)
	tps := uint32(102)
	tbu := uint32(33)
	zrss := uint32(44)
	s = Scale{
		Min:                     pointer.Int32(2),
		Max:                     pointer.Int32(4),
		CooldownSeconds:         &cds,
		LookbackSeconds:         &lbs,
		ReplicasPerScale:        &rps,
		TargetProcessingSeconds: &tps,
		TargetBufferUsage:       &tbu,
		ZeroReplicaSleepSeconds: &zrss,
	}
	assert.Equal(t, int32(2), s.GetMinReplicas())
	assert.Equal(t, int32(4), s.GetMaxReplicas())
	assert.Equal(t, int(cds), s.GetCooldownSeconds())
	assert.Equal(t, int(lbs), s.GetLookbackSeconds())
	assert.Equal(t, int(rps), s.GetReplicasPerScale())
	assert.Equal(t, int(tbu), s.GetTargetBufferUsage())
	assert.Equal(t, int(tps), s.GetTargetProcessingSeconds())
	assert.Equal(t, int(zrss), s.GetZeroReplicaSleepSeconds())
	s.Max = pointer.Int32(500)
	assert.Equal(t, int32(500), s.GetMaxReplicas())
}
