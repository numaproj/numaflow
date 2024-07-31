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

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
)

const (
	testNamespace          = "test-ns"
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
	fakeIsbSvcConfig = dfv1.BufferServiceConfig{
		Redis: &dfv1.RedisConfig{
			URL:         "xxx",
			SentinelURL: "xxxxxxx",
			MasterName:  "mymaster",
			User:        "test-user",
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

	testPipelineWithSideinput = &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pl",
			Namespace: "test-ns",
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
					Name: "p1",
					UDF: &dfv1.UDF{
						Builtin: &dfv1.Function{Name: "cat"},
					},
					SideInputs: []string{"my-sideinput"},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{},
				},
			},
			SideInputs: []dfv1.SideInput{
				{
					Name: "my-sideinput",
					Container: &dfv1.Container{
						Image: "my-image",
					},
					Trigger: &dfv1.SideInputTrigger{
						Schedule: "@every 5s",
					},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "p1"},
				{From: "p1", To: "output"},
			},
			Watermark: dfv1.Watermark{
				Disabled: false,
				MaxDelay: &metav1.Duration{Duration: 5 * time.Second},
			},
		},
	}
)

func init() {
	_ = dfv1.AddToScheme(scheme.Scheme)
	_ = appv1.AddToScheme(scheme.Scheme)
	_ = corev1.AddToScheme(scheme.Scheme)
	_ = batchv1.AddToScheme(scheme.Scheme)
}

func Test_NewReconciler(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	r := NewReconciler(cl, scheme.Scheme, reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig), testFlowImage, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
	_, ok := r.(*pipelineReconciler)
	assert.True(t, ok)
}

func Test_reconcile(t *testing.T) {
	t.Run("test reconcile", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		r := &pipelineReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testPipeline.DeepCopy()
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		vertices := &dfv1.VertexList{}
		selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + testObj.Name)
		err = r.client.List(ctx, vertices, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(vertices.Items))
		jobs := &batchv1.JobList{}
		err = r.client.List(ctx, jobs, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobs.Items))
	})
}

func Test_reconcileEvents(t *testing.T) {

	fakeConfig := reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig)
	t.Run("test reconcile - invalid name", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		r := &pipelineReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testPipeline.DeepCopy()
		testObj.Status.Phase = "Paused"
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		testObj.Name = "very-very-very-loooooooooooooooooooooooooooooooooooong"
		_, err = r.reconcile(ctx, testObj)
		assert.Error(t, err)
		events := getEvents(t, r)
		assert.Contains(t, events, "Normal UpdatePipelinePhase Updated pipeline phase from Paused to Running")
		assert.Contains(t, events, "Warning ReconcilePipelineFailed Failed to reconcile pipeline: the length of the pipeline name plus the vertex name is over the max limit. (very-very-very-loooooooooooooooooooooooooooooooooooong-input), [must be no more than 63 characters]")
	})

	t.Run("test reconcile - duplicate vertex", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		r := &pipelineReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   fakeConfig,
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testPipeline.DeepCopy()
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		testObj.Spec.Vertices = append(testObj.Spec.Vertices, dfv1.AbstractVertex{Name: "input", Source: &dfv1.Source{}})
		_, err = r.reconcile(ctx, testObj)
		assert.Error(t, err)
		events := getEvents(t, r)
		assert.Contains(t, events, "Warning ReconcilePipelineFailed Failed to reconcile pipeline: duplicate vertex name \"input\"")
	})
}

func Test_buildVertices(t *testing.T) {
	r := buildVertices(testPipeline)
	assert.Equal(t, 3, len(r))
	_, existing := r[testPipeline.Name+"-"+testPipeline.Spec.Vertices[0].Name]
	assert.True(t, existing)
	assert.Equal(t, testPipeline.Spec.Watermark.MaxDelay, r[testPipeline.Name+"-"+testPipeline.Spec.Vertices[0].Name].Spec.Watermark.MaxDelay)
}

func Test_buildReducesVertices(t *testing.T) {
	pl := testReducePipeline.DeepCopy()
	pl.Spec.Vertices[1].UDF.GroupBy.Keyed = true
	pl.Spec.Vertices[1].Partitions = ptr.To[int32](2)
	r := buildVertices(pl)
	assert.Equal(t, 6, len(r))
	_, existing := r[pl.Name+"-"+pl.Spec.Vertices[1].Name]
	assert.True(t, existing)
	assert.Equal(t, int32(2), *r[pl.Name+"-"+pl.Spec.Vertices[1].Name].Spec.Replicas)
}

func Test_pauseAndResumePipeline(t *testing.T) {

	t.Run("test normal pipeline", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		r := &pipelineReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testPipeline.DeepCopy()
		testObj.Spec.Vertices[0].Scale.Min = ptr.To[int32](3)
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		_, err = r.pausePipeline(ctx, testObj)
		assert.NoError(t, err)
		v, err := r.findExistingVertices(ctx, testObj)
		assert.NoError(t, err)
		assert.Equal(t, int32(0), *v[testObj.Name+"-"+testObj.Spec.Vertices[0].Name].Spec.Replicas)
		assert.NotNil(t, testObj.Annotations[dfv1.KeyPauseTimestamp])
		testObj.Annotations[dfv1.KeyPauseTimestamp] = ""
		_, err = r.resumePipeline(ctx, testObj)
		assert.NoError(t, err)
		v, err = r.findExistingVertices(ctx, testObj)
		assert.NoError(t, err)
		// when auto-scaling is enabled, while resuming the pipeline, instead of setting the replicas to Scale.Min,
		// we set it to one and let auto-scaling to scale up
		assert.Equal(t, int32(1), *v[testObj.Name+"-"+testObj.Spec.Vertices[0].Name].Spec.Replicas)
		assert.NoError(t, err)
	})

	t.Run("test reduce pipeline", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		r := &pipelineReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testReducePipeline.DeepCopy()
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		_, err = r.pausePipeline(ctx, testObj)
		assert.NoError(t, err)
		_, err = r.findExistingVertices(ctx, testObj)
		assert.NoError(t, err)
		assert.NotNil(t, testObj.Annotations[dfv1.KeyPauseTimestamp])
		testObj.Annotations[dfv1.KeyPauseTimestamp] = ""
		_, err = r.resumePipeline(ctx, testObj)
		assert.NoError(t, err)
		v, err := r.findExistingVertices(ctx, testObj)
		assert.NoError(t, err)
		// reduce UDFs are not autoscalable thus they are scaled manually back to their partition count
		assert.Equal(t, int32(2), *v[testObj.Name+"-"+testObj.Spec.Vertices[2].Name].Spec.Replicas)
		assert.NoError(t, err)
	})
}

func Test_copyVertexLimits(t *testing.T) {
	pl := testPipeline.DeepCopy()
	v := pl.Spec.Vertices[0].DeepCopy()
	copyVertexLimits(pl, v)
	assert.NotNil(t, v.Limits)
	assert.Equal(t, int64(dfv1.DefaultReadBatchSize), int64(*v.Limits.ReadBatchSize))
	one := uint64(1)
	limitJson := `{"readTimeout": "2s"}`
	var pipelineLimit dfv1.PipelineLimits
	err := json.Unmarshal([]byte(limitJson), &pipelineLimit)
	assert.NoError(t, err)
	pipelineLimit.ReadBatchSize = &one
	pl.Spec.Limits = &pipelineLimit
	v1 := new(dfv1.AbstractVertex)
	copyVertexLimits(pl, v1)
	assert.NotNil(t, v1.Limits)
	assert.Equal(t, int64(one), int64(*v1.Limits.ReadBatchSize))
	assert.Equal(t, "2s", v1.Limits.ReadTimeout.Duration.String())
	two := uint64(2)
	vertexLimitJson := `{"readTimeout": "3s"}`
	var vertexLimit dfv1.VertexLimits
	err = json.Unmarshal([]byte(vertexLimitJson), &vertexLimit)
	assert.NoError(t, err)
	v.Limits = &vertexLimit
	v.Limits.ReadBatchSize = &two
	copyVertexLimits(pl, v)
	assert.Equal(t, two, *v.Limits.ReadBatchSize)
	assert.Equal(t, "3s", v.Limits.ReadTimeout.Duration.String())
}

func Test_copyEdges(t *testing.T) {
	t.Run("test copy map", func(t *testing.T) {
		pl := testPipeline.DeepCopy()
		edges := []dfv1.Edge{{From: "input", To: "p1"}}
		result := copyEdges(pl, edges)
		for _, e := range result {
			assert.NotNil(t, e.ToVertexLimits)
			assert.Equal(t, int64(dfv1.DefaultBufferLength), int64(*e.ToVertexLimits.BufferMaxLength))
		}
		onethouand := uint64(1000)
		eighty := uint32(80)
		pl.Spec.Limits = &dfv1.PipelineLimits{BufferMaxLength: &onethouand, BufferUsageLimit: &eighty}
		result = copyEdges(pl, edges)
		for _, e := range result {
			assert.NotNil(t, e.ToVertexLimits)
			assert.NotNil(t, e.ToVertexLimits.BufferMaxLength)
			assert.NotNil(t, e.ToVertexLimits.BufferUsageLimit)
		}

		twothouand := uint64(2000)
		pl.Spec.Vertices[2].Limits = &dfv1.VertexLimits{BufferMaxLength: &twothouand}
		edges = []dfv1.Edge{{From: "p1", To: "output"}}
		result = copyEdges(pl, edges)
		for _, e := range result {
			assert.NotNil(t, e.ToVertexLimits)
			assert.NotNil(t, e.ToVertexLimits.BufferMaxLength)
			assert.Equal(t, twothouand, *e.ToVertexLimits.BufferMaxLength)
			assert.NotNil(t, e.ToVertexLimits.BufferUsageLimit)
			assert.Equal(t, eighty, *e.ToVertexLimits.BufferUsageLimit)
		}
	})

	t.Run("test copy reduce", func(t *testing.T) {
		pl := testReducePipeline.DeepCopy()
		edges := []dfv1.Edge{{From: "p1", To: "p2"}}
		result := copyEdges(pl, edges)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "p1", result[0].From)
		assert.Equal(t, "p2", result[0].To)
		assert.NotNil(t, result[0].ToVertexLimits)
		assert.Equal(t, int64(dfv1.DefaultBufferLength), int64(*result[0].ToVertexLimits.BufferMaxLength))
		assert.Equal(t, int32(2), *result[0].ToVertexPartitionCount)
		assert.Equal(t, int32(1), *result[0].FromVertexPartitionCount)

		edges = []dfv1.Edge{{From: "p2", To: "p3"}}
		result = copyEdges(pl, edges)
		assert.Equal(t, 1, len(result))
		assert.Equal(t, "p2", result[0].From)
		assert.Equal(t, "p3", result[0].To)
		assert.Equal(t, int32(1), *result[0].ToVertexPartitionCount)
		assert.Equal(t, int32(2), *result[0].FromVertexPartitionCount)
	})

}

func Test_buildISBBatchJob(t *testing.T) {
	t.Run("test build ISB batch job", func(t *testing.T) {
		j := buildISBBatchJob(testPipeline, testFlowImage, fakeIsbSvcConfig, "subcmd", []string{"sss"}, "test")
		assert.Equal(t, 1, len(j.Spec.Template.Spec.Containers))
		assert.True(t, len(j.Spec.Template.Spec.Containers[0].Args) > 0)
		assert.Contains(t, j.Name, testPipeline.Name+"-test-")
		envNames := []string{}
		for _, e := range j.Spec.Template.Spec.Containers[0].Env {
			envNames = append(envNames, e.Name)
		}
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelURL)
		assert.Contains(t, envNames, dfv1.EnvISBSvcSentinelMaster)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisSentinelPassword)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisUser)
		assert.Contains(t, envNames, dfv1.EnvISBSvcRedisURL)
	})

	t.Run("test build ISB batch job with pipeline overrides", func(t *testing.T) {
		resources := corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				"memory": resource.MustParse("256Mi"),
			},
		}
		env := corev1.EnvVar{Name: "my-env-name", Value: "my-env-value"}
		podLabels := map[string]string{"my-label-name": "my-label-value"}
		podAnnotations := map[string]string{"my-annotation-name": "my-annotation-value"}
		ttlSecondsAfterFinished := int32(600)
		backoffLimit := int32(50)
		nodeSelector := map[string]string{"my-node-selector-name": "my-node-selector-value"}
		priority := int32(100)
		toleration := corev1.Toleration{
			Key:      "my-toleration-key",
			Operator: "Equal",
			Value:    "my-toleration-value",
			Effect:   "NoSchedule",
		}
		pl := testPipeline.DeepCopy()
		pl.Spec.Templates = &dfv1.Templates{
			JobTemplate: &dfv1.JobTemplate{
				TTLSecondsAfterFinished: &ttlSecondsAfterFinished,
				BackoffLimit:            &backoffLimit,
				ContainerTemplate: &dfv1.ContainerTemplate{
					Resources: resources,
					Env:       []corev1.EnvVar{env},
					SecurityContext: &corev1.SecurityContext{
						Privileged: ptr.To[bool](false),
					},
				},
				AbstractPodTemplate: dfv1.AbstractPodTemplate{
					Metadata: &dfv1.Metadata{
						Annotations: podAnnotations,
						Labels:      podLabels,
					},
					NodeSelector:      nodeSelector,
					Tolerations:       []corev1.Toleration{toleration},
					PriorityClassName: "my-priority-class-name",
					Priority:          &priority,
				},
			},
		}
		j := buildISBBatchJob(pl, testFlowImage, fakeIsbSvcConfig, "subcmd", []string{"sss"}, "test")
		assert.Equal(t, 1, len(j.Spec.Template.Spec.Containers))
		assert.Equal(t, j.Spec.Template.Spec.Containers[0].Resources, resources)
		assert.Greater(t, len(j.Spec.Template.Spec.Containers[0].Env), 1)
		assert.Contains(t, j.Spec.Template.Spec.Containers[0].Env, env)
		assert.NotNil(t, j.Spec.Template.Spec.Containers[0].SecurityContext)
		assert.NotNil(t, j.Spec.Template.Spec.Containers[0].SecurityContext.Privileged)
		assert.False(t, *j.Spec.Template.Spec.Containers[0].SecurityContext.Privileged)
		assert.Equal(t, j.Spec.Template.Labels["my-label-name"], podLabels["my-label-name"])
		assert.Equal(t, j.Spec.Template.Annotations["my-annotation-name"], podAnnotations["my-annotation-name"])
		assert.NotNil(t, j.Spec.TTLSecondsAfterFinished)
		assert.Equal(t, *j.Spec.TTLSecondsAfterFinished, ttlSecondsAfterFinished)
		assert.NotNil(t, j.Spec.BackoffLimit)
		assert.Equal(t, *j.Spec.BackoffLimit, backoffLimit)
		assert.Equal(t, j.Spec.Template.Spec.NodeSelector["my-node-selector-name"], nodeSelector["my-node-selector-name"])
		assert.NotNil(t, j.Spec.Template.Spec.Priority)
		assert.Equal(t, *j.Spec.Template.Spec.Priority, priority)
		assert.Contains(t, j.Spec.Template.Spec.Tolerations, toleration)
		assert.Equal(t, j.Spec.Template.Spec.PriorityClassName, "my-priority-class-name")
	})
}

func Test_needsUpdate(t *testing.T) {
	testObj := testPipeline.DeepCopy()
	assert.True(t, needsUpdate(nil, testObj))
	assert.False(t, needsUpdate(testPipeline, testObj))
	controllerutil.AddFinalizer(testObj, finalizerName)
	assert.True(t, needsUpdate(testPipeline, testObj))
	testobj1 := testObj.DeepCopy()
	assert.False(t, needsUpdate(testObj, testobj1))
}

func Test_cleanupBuffers(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()
	r := &pipelineReconciler{
		client: cl,
		scheme: scheme.Scheme,
		config: reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
		image:  testFlowImage,
		logger: zaptest.NewLogger(t).Sugar(),
	}

	t.Run("test create cleanup buffer job no isbsvc", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		assert.Equal(t, 2, len(testObj.GetAllBuffers()))
		err := r.cleanUpBuffers(ctx, testObj, zaptest.NewLogger(t).Sugar())
		assert.NoError(t, err)
		selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + testObj.Name)
		jobs := &batchv1.JobList{}
		err = r.client.List(ctx, jobs, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(jobs.Items))
	})

	t.Run("test create cleanup buffer job with isbsvc", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testIsbSvc := testNativeRedisIsbSvc.DeepCopy()
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		assert.Nil(t, err)
		err = r.cleanUpBuffers(ctx, testObj, zaptest.NewLogger(t).Sugar())
		assert.NoError(t, err)
		selector, _ := labels.Parse(dfv1.KeyPipelineName + "=" + testObj.Name)
		jobs := &batchv1.JobList{}
		err = r.client.List(ctx, jobs, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobs.Items))
		assert.Contains(t, jobs.Items[0].Name, "cln")
		assert.Equal(t, 0, len(jobs.Items[0].OwnerReferences))
	})
}

func TestCreateOrUpdateDaemon(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()
	r := &pipelineReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
	}

	t.Run("test create or update service", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		err := r.createOrUpdateDaemonService(ctx, testObj)
		assert.NoError(t, err)
		svcList := corev1.ServiceList{}
		err = cl.List(context.Background(), &svcList)
		assert.NoError(t, err)
		assert.Len(t, svcList.Items, 1)
		assert.Equal(t, "test-pl-daemon-svc", svcList.Items[0].Name)
	})

	t.Run("test create or update deployment", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		err := r.createOrUpdateDaemonDeployment(ctx, testObj, fakeIsbSvcConfig)
		assert.NoError(t, err)
		deployList := appv1.DeploymentList{}
		err = cl.List(context.Background(), &deployList)
		assert.NoError(t, err)
		assert.Len(t, deployList.Items, 1)
		assert.Equal(t, "test-pl-daemon", deployList.Items[0].Name)
	})
}

func Test_createOrUpdateSIMDeployments(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()
	r := &pipelineReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
	}

	t.Run("no side inputs", func(t *testing.T) {
		err := r.createOrUpdateSIMDeployments(ctx, testPipeline, fakeIsbSvcConfig)
		assert.NoError(t, err)
		deployList := appv1.DeploymentList{}
		err = cl.List(context.Background(), &deployList, &client.ListOptions{Namespace: testNamespace, LabelSelector: labels.SelectorFromSet(labels.Set{dfv1.KeyComponent: dfv1.ComponentSideInputManager})})
		assert.NoError(t, err)
		assert.Len(t, deployList.Items, 0)
	})

	t.Run("one side input", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.SideInputs = []dfv1.SideInput{
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
		err := r.createOrUpdateSIMDeployments(ctx, testObj, fakeIsbSvcConfig)
		assert.NoError(t, err)
		deployList := appv1.DeploymentList{}
		err = cl.List(context.Background(), &deployList, &client.ListOptions{Namespace: testNamespace, LabelSelector: labels.SelectorFromSet(labels.Set{dfv1.KeyComponent: dfv1.ComponentSideInputManager})})
		assert.NoError(t, err)
		assert.Len(t, deployList.Items, 1)
		assert.Equal(t, testObj.GetSideInputsManagerDeploymentName("s1"), deployList.Items[0].Name)
	})

	t.Run("two side inputs", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.SideInputs = []dfv1.SideInput{
			{
				Name: "s1",
				Container: &dfv1.Container{
					Image: "test",
				},
				Trigger: &dfv1.SideInputTrigger{
					Schedule: "1 * * * *",
				},
			},
			{
				Name: "s2",
				Container: &dfv1.Container{
					Image: "test",
				},
				Trigger: &dfv1.SideInputTrigger{
					Schedule: "1 * * * *",
				},
			},
		}
		err := r.createOrUpdateSIMDeployments(ctx, testObj, fakeIsbSvcConfig)
		assert.NoError(t, err)
		deployList := appv1.DeploymentList{}
		err = cl.List(context.Background(), &deployList, &client.ListOptions{Namespace: testNamespace, LabelSelector: labels.SelectorFromSet(labels.Set{dfv1.KeyComponent: dfv1.ComponentSideInputManager})})
		assert.NoError(t, err)
		assert.Len(t, deployList.Items, 2)
		assert.Equal(t, testObj.GetSideInputsManagerDeploymentName("s1"), deployList.Items[0].Name)
		assert.Equal(t, testObj.GetSideInputsManagerDeploymentName("s2"), deployList.Items[1].Name)
	})

	t.Run("update side inputs", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		testObj.Spec.SideInputs = []dfv1.SideInput{
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
		err := r.createOrUpdateSIMDeployments(ctx, testObj, fakeIsbSvcConfig)
		assert.NoError(t, err)
		testObj.Spec.SideInputs[0].Name = "s2"
		err = r.createOrUpdateSIMDeployments(ctx, testObj, fakeIsbSvcConfig)
		assert.NoError(t, err)
		deployList := appv1.DeploymentList{}
		err = cl.List(context.Background(), &deployList, &client.ListOptions{Namespace: testNamespace, LabelSelector: labels.SelectorFromSet(labels.Set{dfv1.KeyComponent: dfv1.ComponentSideInputManager})})
		assert.NoError(t, err)
		assert.Len(t, deployList.Items, 1)
		assert.Equal(t, testObj.GetSideInputsManagerDeploymentName("s2"), deployList.Items[0].Name)
	})
}

func getEvents(t *testing.T, reconciler *pipelineReconciler) []string {
	t.Helper()
	c := reconciler.recorder.(*record.FakeRecorder).Events
	close(c)
	events := make([]string, len(c))
	for msg := range c {
		events = append(events, msg)
	}
	return events
}

func Test_copyVertexTemplate(t *testing.T) {
	t.Run("no template", func(t *testing.T) {
		pl := &dfv1.Pipeline{}
		vtx := &dfv1.AbstractVertex{}
		copyVertexTemplate(pl, vtx)
		assert.Nil(t, vtx.Metadata)
		assert.Nil(t, vtx.AbstractPodTemplate.Affinity)
		assert.Nil(t, vtx.AbstractPodTemplate.Tolerations)
		assert.Nil(t, vtx.AbstractPodTemplate.Priority)
		assert.Nil(t, vtx.AbstractPodTemplate.DNSConfig)
		assert.Nil(t, vtx.AbstractPodTemplate.SecurityContext)
	})

	t.Run("template defined with no vertex customization", func(t *testing.T) {
		pl := &dfv1.Pipeline{
			Spec: dfv1.PipelineSpec{
				Templates: &dfv1.Templates{
					VertexTemplate: &dfv1.VertexTemplate{
						AbstractPodTemplate: dfv1.AbstractPodTemplate{
							Metadata: &dfv1.Metadata{
								Labels: map[string]string{
									"label1": "value1",
									"label2": "value2",
								},
								Annotations: map[string]string{
									"annotation1": "value1",
									"annotation2": "value2",
								},
							},
							PriorityClassName: "test",
							DNSConfig: &corev1.PodDNSConfig{
								Nameservers: []string{"1.1.1.1", "8.8.8.8"},
							},
							SecurityContext: &corev1.PodSecurityContext{
								RunAsUser: ptr.To[int64](1000),
							},
						},
						ContainerTemplate: &dfv1.ContainerTemplate{
							ImagePullPolicy: corev1.PullNever,
							Env: []corev1.EnvVar{
								{Name: "ENV1", Value: "VALUE1"},
								{Name: "ENV2", Value: "VALUE2"},
							},
						},
						InitContainerTemplate: &dfv1.ContainerTemplate{
							ImagePullPolicy: corev1.PullNever,
							Env: []corev1.EnvVar{
								{Name: "ENV3", Value: "VALUE3"},
								{Name: "ENV4", Value: "VALUE4"},
							},
						},
					},
				},
			},
		}
		vtx := &dfv1.AbstractVertex{}
		copyVertexTemplate(pl, vtx)
		assert.NotNil(t, vtx.Metadata)
		assert.Equal(t, 2, len(vtx.Metadata.Labels))
		assert.Equal(t, "value1", vtx.Metadata.Labels["label1"])
		assert.Equal(t, "value2", vtx.Metadata.Labels["label2"])
		assert.Equal(t, 2, len(vtx.Metadata.Annotations))
		assert.Equal(t, "value1", vtx.Metadata.Annotations["annotation1"])
		assert.Equal(t, "value2", vtx.Metadata.Annotations["annotation2"])
		assert.Equal(t, "test", vtx.PriorityClassName)
		assert.Equal(t, 2, len(vtx.DNSConfig.Nameservers))
		assert.Equal(t, "1.1.1.1", vtx.DNSConfig.Nameservers[0])
		assert.Equal(t, "8.8.8.8", vtx.DNSConfig.Nameservers[1])
		assert.Equal(t, int64(1000), *vtx.SecurityContext.RunAsUser)
		assert.Equal(t, corev1.PullNever, vtx.ContainerTemplate.ImagePullPolicy)
		assert.Equal(t, 2, len(vtx.ContainerTemplate.Env))
		assert.Equal(t, "ENV1", vtx.ContainerTemplate.Env[0].Name)
		assert.Equal(t, "VALUE1", vtx.ContainerTemplate.Env[0].Value)
		assert.Equal(t, corev1.PullNever, vtx.InitContainerTemplate.ImagePullPolicy)
		assert.Equal(t, 2, len(vtx.InitContainerTemplate.Env))
		assert.Equal(t, "ENV3", vtx.InitContainerTemplate.Env[0].Name)
		assert.Equal(t, "VALUE3", vtx.InitContainerTemplate.Env[0].Value)
	})

	t.Run("template with vertex override", func(t *testing.T) {
		pl := &dfv1.Pipeline{
			Spec: dfv1.PipelineSpec{
				Templates: &dfv1.Templates{
					VertexTemplate: &dfv1.VertexTemplate{
						AbstractPodTemplate: dfv1.AbstractPodTemplate{
							Metadata: &dfv1.Metadata{
								Labels: map[string]string{
									"label1": "value1",
									"label2": "value2",
								},
								Annotations: map[string]string{
									"annotation1": "value1",
									"annotation2": "value2",
								},
							},
							PriorityClassName: "test",
							DNSConfig: &corev1.PodDNSConfig{
								Nameservers: []string{"1.1.1.1", "8.8.8.8", "4.4.4.4"},
							},
							SecurityContext: &corev1.PodSecurityContext{
								RunAsUser: ptr.To[int64](1000),
							},
						},
						ContainerTemplate: &dfv1.ContainerTemplate{
							ImagePullPolicy: corev1.PullNever,
							Env: []corev1.EnvVar{
								{Name: "ENV1", Value: "VALUE1"},
								{Name: "ENV2", Value: "VALUE2"},
							},
						},
						InitContainerTemplate: &dfv1.ContainerTemplate{
							ImagePullPolicy: corev1.PullNever,
							Env: []corev1.EnvVar{
								{Name: "ENV3", Value: "VALUE3"},
								{Name: "ENV4", Value: "VALUE4"},
							},
							EnvFrom: []corev1.EnvFromSource{
								{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "test"}}},
							},
						},
					},
				},
			},
		}
		vtx := &dfv1.AbstractVertex{
			AbstractPodTemplate: dfv1.AbstractPodTemplate{
				Metadata: &dfv1.Metadata{
					Labels: map[string]string{
						"existing-label": "existing-value",
						"label2":         "value22",
					},
					Annotations: map[string]string{
						"existing-annotation": "existing-value",
						"annotation2":         "value22",
					},
				},
				PriorityClassName: "test2",
				DNSConfig: &corev1.PodDNSConfig{
					Nameservers: []string{"1.1.1.1", "8.8.8.8", "9.9.9.9"},
				},
				SecurityContext: &corev1.PodSecurityContext{
					RunAsUser: ptr.To[int64](2000),
				},
			},
			ContainerTemplate: &dfv1.ContainerTemplate{
				ImagePullPolicy: corev1.PullAlways,
				Env: []corev1.EnvVar{
					{Name: "ENV1", Value: "VALUE11"},
					{Name: "ENV3", Value: "VALUE33"},
				},
			},
			InitContainerTemplate: &dfv1.ContainerTemplate{
				Env: []corev1.EnvVar{
					{Name: "ENV3", Value: "VALUE33"},
					{Name: "ENV4", Value: "VALUE44"},
				},
				SecurityContext: &corev1.SecurityContext{
					Capabilities: &corev1.Capabilities{
						Add: []corev1.Capability{"NET_ADMIN"},
					},
				},
				EnvFrom: []corev1.EnvFromSource{},
			},
		}
		copyVertexTemplate(pl, vtx)
		assert.NotNil(t, vtx.Metadata)
		assert.Equal(t, 3, len(vtx.Metadata.Labels))
		assert.Equal(t, "existing-value", vtx.Metadata.Labels["existing-label"])
		assert.Equal(t, "value1", vtx.Metadata.Labels["label1"])
		assert.Equal(t, "value22", vtx.Metadata.Labels["label2"])
		assert.Equal(t, 3, len(vtx.Metadata.Annotations))
		assert.Equal(t, "existing-value", vtx.Metadata.Annotations["existing-annotation"])
		assert.Equal(t, "value1", vtx.Metadata.Annotations["annotation1"])
		assert.Equal(t, "value22", vtx.Metadata.Annotations["annotation2"])
		assert.Equal(t, "test2", vtx.PriorityClassName)
		assert.Equal(t, 3, len(vtx.DNSConfig.Nameservers))
		assert.Equal(t, "1.1.1.1", vtx.DNSConfig.Nameservers[0])
		assert.Equal(t, "8.8.8.8", vtx.DNSConfig.Nameservers[1])
		assert.Equal(t, "9.9.9.9", vtx.DNSConfig.Nameservers[2])
		assert.Equal(t, int64(2000), *vtx.SecurityContext.RunAsUser)
		assert.Equal(t, corev1.PullAlways, vtx.ContainerTemplate.ImagePullPolicy)
		assert.Equal(t, 2, len(vtx.ContainerTemplate.Env))
		assert.Equal(t, "ENV1", vtx.ContainerTemplate.Env[0].Name)
		assert.Equal(t, "VALUE11", vtx.ContainerTemplate.Env[0].Value)
		assert.Equal(t, "ENV3", vtx.ContainerTemplate.Env[1].Name)
		assert.Equal(t, "VALUE33", vtx.ContainerTemplate.Env[1].Value)
		assert.Nil(t, vtx.ContainerTemplate.SecurityContext)
		assert.Equal(t, corev1.PullNever, vtx.InitContainerTemplate.ImagePullPolicy)
		assert.Equal(t, 2, len(vtx.InitContainerTemplate.Env))
		assert.Equal(t, "ENV3", vtx.InitContainerTemplate.Env[0].Name)
		assert.Equal(t, "VALUE33", vtx.InitContainerTemplate.Env[0].Value)
		assert.Equal(t, "ENV4", vtx.InitContainerTemplate.Env[1].Name)
		assert.Equal(t, "VALUE44", vtx.InitContainerTemplate.Env[1].Value)
		assert.NotNil(t, vtx.InitContainerTemplate.SecurityContext)
		assert.Equal(t, 1, len(vtx.InitContainerTemplate.SecurityContext.Capabilities.Add))
		assert.Equal(t, 1, len(vtx.InitContainerTemplate.EnvFrom))
		assert.NotNil(t, vtx.InitContainerTemplate.EnvFrom[0].SecretRef)
		assert.Equal(t, "test", vtx.InitContainerTemplate.EnvFrom[0].SecretRef.Name)
	})
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
		r := &pipelineReconciler{
			client:   cl,
			scheme:   scheme.Scheme,
			config:   reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			image:    testFlowImage,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
		}
		testObj := testPipelineWithSideinput.DeepCopy()
		_, err = r.reconcile(ctx, testObj)
		assert.NoError(t, err)
		err = checkChildrenResourceStatus(ctx, cl, testObj)
		assert.NoError(t, err)
		for _, c := range testObj.Status.Conditions {
			if c.Type == string(dfv1.PipelineConditionDaemonServiceHealthy) {
				assert.Equal(t, string(corev1.ConditionTrue), string(c.Status))
			} else if c.Type == string(dfv1.PipelineConditionSideInputsManagersHealthy) {
				assert.Equal(t, string(corev1.ConditionTrue), string(c.Status))
			} else if c.Type == string(dfv1.PipelineConditionVerticesHealthy) {
				assert.Equal(t, string(corev1.ConditionFalse), string(c.Status))
			}
		}
	})
}
