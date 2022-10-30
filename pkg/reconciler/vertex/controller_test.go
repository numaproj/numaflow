package vertex

import (
	"context"
	"fmt"
	"strings"
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/reconciler/vertex/scaling"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testNamespace          = "test-ns"
	testVertexSpecName     = "p1"
	testPipelineName       = "test-pl"
	testVertexName         = testPipelineName + "-" + testVertexSpecName
	testVersion            = "6.2.6"
	testImage              = "test-image"
	testSImage             = "test-s-image"
	testRedisExporterImage = "test-r-exporter-image"
	testFlowImage          = "test-d-iamge"
)

var (
	testNativeRedisIsbSvc = &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      dfv1.DefaultISBSvcName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{
			Redis: &dfv1.RedisBufferService{
				Native: &dfv1.NativeRedis{
					Version: testVersion,
				},
			},
		},
	}

	fakeConfig = &reconciler.GlobalConfig{
		ISBSvc: &reconciler.ISBSvcConfig{
			Redis: &reconciler.RedisConfig{
				Versions: []reconciler.RedisVersion{
					{
						Version:            testVersion,
						RedisImage:         testImage,
						SentinelImage:      testSImage,
						RedisExporterImage: testRedisExporterImage,
					},
				},
			},
		},
	}

	testReplicas = int32(1)
	testVertex   = &dfv1.Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testVertexName,
		},
		Spec: dfv1.VertexSpec{
			Replicas:     &testReplicas,
			FromEdges:    []dfv1.Edge{{From: "input", To: testVertexSpecName}},
			ToEdges:      []dfv1.Edge{{From: testVertexSpecName, To: "output"}},
			PipelineName: testPipelineName,
			AbstractVertex: dfv1.AbstractVertex{
				Name: testVertexSpecName,
			},
		},
	}

	testSrcVertex = &dfv1.Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      "test-pl-p1",
		},
		Spec: dfv1.VertexSpec{
			Replicas:     &testReplicas,
			ToEdges:      []dfv1.Edge{{From: "input", To: "p1"}},
			PipelineName: testPipelineName,
			AbstractVertex: dfv1.AbstractVertex{
				Name:   "input",
				Source: &dfv1.Source{},
			},
		},
	}

	fakeIsbSvcConfig = dfv1.BufferServiceConfig{
		Redis: &dfv1.RedisConfig{
			URL:         "xxx",
			SentinelURL: "xxxxxxx",
			User:        "test-user",
			MasterName:  "mymaster",
			Password: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "test-name",
				},
				Key: "test-key",
			},
			SentinelPassword: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "test-name",
				},
				Key: "test-key",
			},
		},
	}

	testPipeline = &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testPipelineName,
			Namespace: testNamespace,
		},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name:   "input",
					Source: &dfv1.Source{},
				},
				{
					Name: "p1",
					UDF:  &dfv1.UDF{},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "p1"},
				{From: "p1", To: "output"},
			},
		},
	}
)

func init() {
	_ = dfv1.AddToScheme(scheme.Scheme)
	_ = appv1.AddToScheme(scheme.Scheme)
	_ = corev1.AddToScheme(scheme.Scheme)
}

func Test_NewReconciler(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	r := NewReconciler(cl, scheme.Scheme, fakeConfig, testFlowImage, scaling.NewScaler(cl), zaptest.NewLogger(t).Sugar())
	_, ok := r.(*vertexReconciler)
	assert.True(t, ok)
}

func Test_BuildPodSpec(t *testing.T) {
	t.Run("test source", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			image:  testFlowImage,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		testObj := testSrcVertex.DeepCopy()
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 1, len(spec.Containers))
		envNames := []string{}
		for _, e := range spec.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelURL)
		assert.Contains(t, envNames, dfv1.EnvISBSvcSentinelMaster)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisUser)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisURL)
		assert.Contains(t, envNames, dfv1.EnvWatermarkDisabled)
		assert.Contains(t, envNames, dfv1.EnvWatermarkMaxDelay)
		argStr := strings.Join(spec.InitContainers[0].Args, " ")
		assert.Contains(t, argStr, "--buffers=")
		for _, b := range testObj.GetToBuffers() {
			assert.Contains(t, argStr, fmt.Sprintf("%s=%s", b.Name, b.Type))
		}
	})

	t.Run("test sink", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			image:  testFlowImage,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		testObj := testVertex.DeepCopy()
		testObj.Name = "test-pl-output"
		testObj.Spec.Name = "output"
		testObj.Spec.Sink = &dfv1.Sink{}
		testObj.Spec.FromEdges = []dfv1.Edge{{From: "p1", To: "output"}}
		testObj.Spec.ToEdges = []dfv1.Edge{}
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 1, len(spec.Containers))
		envNames := []string{}
		for _, e := range spec.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelURL)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcSentinelMaster)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisUser)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisURL)
		assert.Contains(t, envNames, dfv1.EnvWatermarkDisabled)
		assert.Contains(t, envNames, dfv1.EnvWatermarkMaxDelay)
		argStr := strings.Join(spec.InitContainers[0].Args, " ")
		assert.Contains(t, argStr, "--buffers=")
		for _, b := range testObj.GetFromBuffers() {
			assert.Contains(t, argStr, fmt.Sprintf("%s=%s", b.Name, b.Type))
		}
	})

	t.Run("test user defined sink", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			image:  testFlowImage,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		testObj := testVertex.DeepCopy()
		testObj.Name = "test-pl-output"
		testObj.Spec.Name = "output"
		testObj.Spec.Sink = &dfv1.Sink{
			UDSink: &dfv1.UDSink{
				Container: dfv1.Container{
					Image:   "image",
					Command: []string{"cmd"},
					Args:    []string{"arg0"},
				},
			},
		}
		testObj.Spec.FromEdges = []dfv1.Edge{{From: "p1", To: "output"}}
		testObj.Spec.ToEdges = []dfv1.Edge{}
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 2, len(spec.Containers))
		assert.Equal(t, "image", spec.Containers[1].Image)
		assert.Equal(t, 1, len(spec.Containers[1].Command))
		assert.Equal(t, "cmd", spec.Containers[1].Command[0])
		assert.Equal(t, 1, len(spec.Containers[1].Args))
		assert.Equal(t, "arg0", spec.Containers[1].Args[0])
	})

	t.Run("test udf", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			image:  testFlowImage,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.UDF = &dfv1.UDF{
			Builtin: &dfv1.Function{
				Name: "cat",
			},
		}
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 2, len(spec.Containers))
		envNames := []string{}
		for _, e := range spec.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelURL)
		assert.Contains(t, envNames, dfv1.EnvISBSvcSentinelMaster)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisUser)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisURL)
		assert.Contains(t, envNames, dfv1.EnvWatermarkDisabled)
		assert.Contains(t, envNames, dfv1.EnvWatermarkMaxDelay)
		argStr := strings.Join(spec.InitContainers[0].Args, " ")
		assert.Contains(t, argStr, "--buffers=")
		for _, b := range testObj.GetFromBuffers() {
			assert.Contains(t, argStr, fmt.Sprintf("%s=%s", b.Name, b.Type))
		}
		for _, b := range testObj.GetToBuffers() {
			assert.Contains(t, argStr, fmt.Sprintf("%s=%s", b.Name, b.Type))
		}
	})
}

func Test_reconcile(t *testing.T) {
	t.Run("test reconcile source", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		testPl := testPipeline.DeepCopy()
		err = cl.Create(ctx, testPl)
		assert.Nil(t, err)
		r := &vertexReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			image:  testFlowImage,
			scaler: scaling.NewScaler(cl),
			logger: zaptest.NewLogger(t).Sugar(),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.Source = &dfv1.Source{
			HTTP: &dfv1.HTTPSource{
				Service: true,
			},
		}
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		pods := &corev1.PodList{}
		selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + testPipelineName + "," + dfv1.KeyVertexName + "=" + testVertexSpecName)
		err = r.client.List(ctx, pods, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(pods.Items))
		assert.True(t, strings.HasPrefix(pods.Items[0].Name, testVertexName+"-0-"))
		assert.Equal(t, 1, len(pods.Items[0].Spec.Containers))
		svcs := &corev1.ServiceList{}
		err = r.client.List(ctx, svcs, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(svcs.Items))
		svcNames := []string{}
		for _, s := range svcs.Items {
			svcNames = append(svcNames, s.Name)
		}
		assert.Contains(t, svcNames, testObj.Name)
		assert.Contains(t, svcNames, testObj.GetHeadlessServiceName())
	})

	t.Run("test reconcile sink", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		testPl := testPipeline.DeepCopy()
		err = cl.Create(ctx, testPl)
		assert.Nil(t, err)
		r := &vertexReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			image:  testFlowImage,
			scaler: scaling.NewScaler(cl),
			logger: zaptest.NewLogger(t).Sugar(),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.Sink = &dfv1.Sink{}
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		pods := &corev1.PodList{}
		selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + testPipelineName + "," + dfv1.KeyVertexName + "=" + testVertexSpecName)
		err = r.client.List(ctx, pods, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(pods.Items))
		assert.True(t, strings.HasPrefix(pods.Items[0].Name, testVertexName+"-0-"))
		assert.Equal(t, 1, len(pods.Items[0].Spec.Containers))
	})

	t.Run("test reconcile udf", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		testPl := testPipeline.DeepCopy()
		err = cl.Create(ctx, testPl)
		assert.Nil(t, err)
		r := &vertexReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			image:  testFlowImage,
			scaler: scaling.NewScaler(cl),
			logger: zaptest.NewLogger(t).Sugar(),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.UDF = &dfv1.UDF{
			Builtin: &dfv1.Function{
				Name: "cat",
			},
		}
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		pods := &corev1.PodList{}
		selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + testPipelineName + "," + dfv1.KeyVertexName + "=" + testVertexSpecName)
		err = r.client.List(ctx, pods, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(pods.Items))
		assert.True(t, strings.HasPrefix(pods.Items[0].Name, testVertexName+"-0-"))
		assert.Equal(t, 2, len(pods.Items[0].Spec.Containers))
	})
}
