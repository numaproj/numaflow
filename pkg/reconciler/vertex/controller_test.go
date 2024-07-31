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

package vertex

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/reconciler/vertex/scaling"
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

	fakeGlobalISBSvcConfig = &reconciler.ISBSvcConfig{
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
	}

	testReplicas = int32(1)
	testVertex   = &dfv1.Vertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testVertexName,
		},
		Spec: dfv1.VertexSpec{
			Replicas:     &testReplicas,
			FromEdges:    []dfv1.CombinedEdge{{Edge: dfv1.Edge{From: "input", To: testVertexSpecName}}},
			ToEdges:      []dfv1.CombinedEdge{{Edge: dfv1.Edge{From: testVertexSpecName, To: "output"}}},
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
			ToEdges:      []dfv1.CombinedEdge{{Edge: dfv1.Edge{From: "input", To: "p1"}}},
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
	r := NewReconciler(cl, scheme.Scheme, reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig), testFlowImage, scaling.NewScaler(cl), zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
	_, ok := r.(*vertexReconciler)
	assert.True(t, ok)
}

func Test_BuildPodSpec(t *testing.T) {

	fakeConfig := reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig)
	t.Run("test source", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testSrcVertex.DeepCopy()
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 1, len(spec.Containers))
		var envNames []string
		for _, e := range spec.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelURL)
		assert.Contains(t, envNames, dfv1.EnvISBSvcSentinelMaster)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisUser)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisURL)
		argStr := strings.Join(spec.InitContainers[0].Args, " ")
		assert.Contains(t, argStr, "--buffers=")
		assert.Contains(t, argStr, strings.Join(testObj.OwnedBuffers(), ","))
		assert.Contains(t, argStr, "--buckets=")
		assert.Contains(t, argStr, strings.Join(testObj.GetFromBuckets(), ","))
		assert.Contains(t, argStr, strings.Join(testObj.GetToBuckets(), ","))
	})

	t.Run("test source with transformer", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testSrcVertex.DeepCopy()
		testObj.Spec.Source = &dfv1.Source{
			HTTP: &dfv1.HTTPSource{},
			UDTransformer: &dfv1.UDTransformer{
				Container: &dfv1.Container{
					Image: "my-image",
				},
			},
		}
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig, 2)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 2, len(spec.Containers))
	})

	t.Run("test user-defined source with transformer", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testSrcVertex.DeepCopy()
		testObj.Spec.Source = &dfv1.Source{
			UDSource: &dfv1.UDSource{
				Container: &dfv1.Container{
					Image: "image",
				},
			},
			UDTransformer: &dfv1.UDTransformer{
				Container: &dfv1.Container{
					Image: "my-image",
				},
			},
		}
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig, 2)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 3, len(spec.Containers))
	})

	t.Run("test sink", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testVertex.DeepCopy()
		testObj.Name = "test-pl-output"
		testObj.Spec.Name = "output"
		testObj.Spec.Sink = &dfv1.Sink{}
		testObj.Spec.FromEdges = []dfv1.CombinedEdge{{Edge: dfv1.Edge{From: "p1", To: "output"}}}
		testObj.Spec.ToEdges = []dfv1.CombinedEdge{}
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 1, len(spec.Containers))
		var envNames []string
		for _, e := range spec.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelURL)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcSentinelMaster)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisUser)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisURL)
		argStr := strings.Join(spec.InitContainers[0].Args, " ")
		assert.Contains(t, argStr, "--buffers=")
		assert.Contains(t, argStr, strings.Join(testObj.OwnedBuffers(), ","))
		assert.Contains(t, argStr, "--buckets=")
		assert.Contains(t, argStr, strings.Join(testObj.GetFromBuckets(), ","))
		assert.Contains(t, argStr, strings.Join(testObj.GetToBuckets(), ","))
	})

	t.Run("test user-defined sink", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testVertex.DeepCopy()
		testObj.Name = "test-pl-output"
		testObj.Spec.Name = "output"
		testObj.Spec.Sink = &dfv1.Sink{
			AbstractSink: dfv1.AbstractSink{
				UDSink: &dfv1.UDSink{
					Container: dfv1.Container{
						Image:   "image",
						Command: []string{"cmd"},
						Args:    []string{"arg0"},
					},
				},
			},
		}
		testObj.Spec.FromEdges = []dfv1.CombinedEdge{{Edge: dfv1.Edge{From: "p1", To: "output"}}}
		testObj.Spec.ToEdges = []dfv1.CombinedEdge{}
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 2, len(spec.Containers))
		assert.Equal(t, "image", spec.Containers[1].Image)
		assert.Equal(t, 1, len(spec.Containers[1].Command))
		assert.Equal(t, "cmd", spec.Containers[1].Command[0])
		assert.Equal(t, 1, len(spec.Containers[1].Args))
		assert.Equal(t, "arg0", spec.Containers[1].Args[0])
	})

	t.Run("test map udf", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.UDF = &dfv1.UDF{
			Builtin: &dfv1.Function{
				Name: "cat",
			},
		}
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig, 0)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 2, len(spec.Containers))
		var envNames []string
		for _, e := range spec.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelURL)
		assert.Contains(t, envNames, dfv1.EnvISBSvcSentinelMaster)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisUser)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisURL)
		argStr := strings.Join(spec.InitContainers[0].Args, " ")
		assert.Contains(t, argStr, "--buffers=")
		assert.Contains(t, argStr, strings.Join(testObj.OwnedBuffers(), ","))
		assert.Contains(t, argStr, "--buckets=")
		assert.Contains(t, argStr, strings.Join(testObj.GetFromBuckets(), ","))
		assert.Contains(t, argStr, strings.Join(testObj.GetToBuckets(), ","))
	})

	t.Run("test reduce udf", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := &vertexReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testVertex.DeepCopy()
		volSize, _ := resource.ParseQuantity("1Gi")
		testObj.Spec.UDF = &dfv1.UDF{
			Container: &dfv1.Container{
				Image: "my-image",
			},
			GroupBy: &dfv1.GroupBy{
				Storage: &dfv1.PBQStorage{
					PersistentVolumeClaim: &dfv1.PersistenceStrategy{
						VolumeSize: &volSize,
					},
				},
			},
		}
		spec, err := r.buildPodSpec(testObj, testPipeline, fakeIsbSvcConfig, 2)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.InitContainers))
		assert.Equal(t, 2, len(spec.Containers))
		containsPVC := false
		containsPVCMount := false
		for _, v := range spec.Volumes {
			if v.Name == "pbq-vol" {
				containsPVC = true
			}
		}
		assert.True(t, containsPVC)
		for _, m := range spec.Containers[0].VolumeMounts {
			if m.MountPath == dfv1.PathPBQMount {
				containsPVCMount = true
			}
		}
		assert.True(t, containsPVCMount)
	})
}

func Test_reconcile(t *testing.T) {
	fakeConfig := reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig)
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
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			scaler:   scaling.NewScaler(cl),
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.Source = &dfv1.Source{
			HTTP: &dfv1.HTTPSource{
				Service: true,
			},
			UDTransformer: &dfv1.UDTransformer{
				Container: &dfv1.Container{
					Image: "my-image",
				},
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
		svcs := &corev1.ServiceList{}
		err = r.client.List(ctx, svcs, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(svcs.Items))
		var svcNames []string
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
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			scaler:   scaling.NewScaler(cl),
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
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
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			scaler:   scaling.NewScaler(cl),
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
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

	t.Run("test reconcile vertex with customization", func(t *testing.T) {
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
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			scaler:   scaling.NewScaler(cl),
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.Sink = &dfv1.Sink{}
		testObj.Spec.ContainerTemplate = &dfv1.ContainerTemplate{
			SecurityContext: &corev1.SecurityContext{
				Capabilities: &corev1.Capabilities{
					Add: []corev1.Capability{"NET_ADMIN"},
				},
			},
		}
		testObj.Spec.InitContainerTemplate = &dfv1.ContainerTemplate{
			SecurityContext: &corev1.SecurityContext{
				Capabilities: &corev1.Capabilities{
					Add: []corev1.Capability{"NET_ADMIN"},
				},
			},
		}
		testObj.Spec.Metadata = &dfv1.Metadata{
			Annotations: map[string]string{"a": "a1"},
			Labels:      map[string]string{"b": "b1"},
		}
		testObj.Spec.PriorityClassName = "test"
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		pods := &corev1.PodList{}
		selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + testPipelineName + "," + dfv1.KeyVertexName + "=" + testVertexSpecName)
		err = r.client.List(ctx, pods, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(pods.Items))
		assert.True(t, strings.HasPrefix(pods.Items[0].Name, testVertexName+"-0-"))
		assert.Equal(t, 1, len(pods.Items[0].Spec.Containers))
		assert.Equal(t, "test", pods.Items[0].Spec.PriorityClassName)
		assert.Equal(t, "a1", pods.Items[0].Annotations["a"])
		assert.Equal(t, "b1", pods.Items[0].Labels["b"])
		assert.NotNil(t, pods.Items[0].Spec.Containers[0].SecurityContext)
		assert.NotNil(t, pods.Items[0].Spec.Containers[0].SecurityContext.Capabilities)
		assert.Equal(t, 1, len(pods.Items[0].Spec.Containers[0].SecurityContext.Capabilities.Add))
		assert.NotNil(t, pods.Items[0].Spec.InitContainers[0].SecurityContext)
		assert.NotNil(t, pods.Items[0].Spec.InitContainers[0].SecurityContext.Capabilities)
		assert.Equal(t, 1, len(pods.Items[0].Spec.InitContainers[0].SecurityContext.Capabilities.Add))
	})

	t.Run("test reconcile udf with side inputs", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		testPl := testPipeline.DeepCopy()
		testPl.Spec.SideInputs = []dfv1.SideInput{
			{
				Name: "s1",
				Container: &dfv1.Container{
					Image: "test",
				},
				Trigger: &dfv1.SideInputTrigger{
					Schedule: "1 * * * *",
				},
			},
		}
		testPl.Spec.Vertices[1].SideInputs = []string{"s1"}
		err = cl.Create(ctx, testPl)
		assert.Nil(t, err)
		r := &vertexReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			scaler:   scaling.NewScaler(cl),
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.UDF = &dfv1.UDF{
			Builtin: &dfv1.Function{
				Name: "cat",
			},
		}
		testObj.Spec.SideInputs = []string{"s1"}
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		pods := &corev1.PodList{}
		selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + testPipelineName + "," + dfv1.KeyVertexName + "=" + testVertexSpecName)
		err = r.client.List(ctx, pods, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(pods.Items))
		assert.True(t, strings.HasPrefix(pods.Items[0].Name, testVertexName+"-0-"))
		assert.Equal(t, 3, len(pods.Items[0].Spec.Containers))
		assert.Equal(t, 2, len(pods.Items[0].Spec.InitContainers))
	})
}

func Test_reconcileEvents(t *testing.T) {
	t.Run("test reconcile - isbsvc doesn't exist", func(t *testing.T) {
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
			client:   cl,
			scheme:   scheme.Scheme,
			config:   reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			image:    testFlowImage,
			scaler:   scaling.NewScaler(cl),
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.UDF = &dfv1.UDF{
			Builtin: &dfv1.Function{
				Name: "cat",
			},
		}
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		events := getEvents(r)
		assert.Contains(t, events, "Normal CreateSvcSuccess Succeeded to create service test-pl-p1-headless")
	})
}

func getEvents(reconciler *vertexReconciler) []string {
	c := reconciler.recorder.(*record.FakeRecorder).Events
	close(c)
	events := make([]string, len(c))
	for msg := range c {
		events = append(events, msg)
	}
	return events
}

func Test_checkChildrenResourceStatus(t *testing.T) {
	t.Run("test check children resource status", func(t *testing.T) {
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
			client:   cl,
			scheme:   scheme.Scheme,
			config:   reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			image:    testFlowImage,
			scaler:   scaling.NewScaler(cl),
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testVertex.DeepCopy()
		testObj.Spec.UDF = &dfv1.UDF{
			Builtin: &dfv1.Function{
				Name: "cat",
			},
		}
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		err = checkChildrenResourceStatus(ctx, r.client, testObj)
		assert.NoError(t, err)
		for _, c := range testObj.Status.Conditions {
			if c.Type == string(dfv1.VertexConditionPodsHealthy) {
				assert.Equal(t, string(corev1.ConditionTrue), string(c.Status))
			}
		}
	})
}
