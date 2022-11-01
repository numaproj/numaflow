package pipeline

import (
	"context"
	"k8s.io/apimachinery/pkg/api/resource"
	"testing"

	"github.com/goccy/go-json"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
)

func init() {
	_ = dfv1.AddToScheme(scheme.Scheme)
	_ = appv1.AddToScheme(scheme.Scheme)
	_ = corev1.AddToScheme(scheme.Scheme)
	_ = batchv1.AddToScheme(scheme.Scheme)
}

func Test_NewReconciler(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	r := NewReconciler(cl, scheme.Scheme, fakeConfig, testFlowImage, zaptest.NewLogger(t).Sugar())
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
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			image:  testFlowImage,
			logger: zaptest.NewLogger(t).Sugar(),
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

func Test_buildVertices(t *testing.T) {
	r := buildVertices(testPipeline)
	assert.Equal(t, 3, len(r))
	_, existing := r[testPipeline.Name+"-"+testPipeline.Spec.Vertices[0].Name]
	assert.True(t, existing)
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

func Test_copyEdgeLimits(t *testing.T) {
	pl := testPipeline.DeepCopy()
	edges := []dfv1.Edge{{From: "in", To: "out"}}
	result := copyEdgeLimits(pl, edges)
	for _, e := range result {
		assert.NotNil(t, e.Limits)
		assert.Equal(t, int64(dfv1.DefaultBufferLength), int64(*e.Limits.BufferMaxLength))
	}
	onethouand := uint64(1000)
	eighty := uint32(80)
	pl.Spec.Limits = &dfv1.PipelineLimits{BufferMaxLength: &onethouand, BufferUsageLimit: &eighty}
	result = copyEdgeLimits(pl, edges)
	for _, e := range result {
		assert.NotNil(t, e.Limits)
		assert.NotNil(t, e.Limits.BufferMaxLength)
		assert.NotNil(t, e.Limits.BufferUsageLimit)
	}

	twothouand := uint64(2000)
	edges = []dfv1.Edge{{From: "in", To: "out", Limits: &dfv1.EdgeLimits{BufferMaxLength: &twothouand}}}
	result = copyEdgeLimits(pl, edges)
	for _, e := range result {
		assert.NotNil(t, e.Limits)
		assert.NotNil(t, e.Limits.BufferMaxLength)
		assert.Equal(t, twothouand, *e.Limits.BufferMaxLength)
		assert.NotNil(t, e.Limits.BufferUsageLimit)
		assert.Equal(t, eighty, *e.Limits.BufferUsageLimit)
	}
}

func Test_buildISBBatchJob(t *testing.T) {
	t.Run("test build ISB batch job", func(t *testing.T) {
		j := buildISBBatchJob(testPipeline, testFlowImage, fakeIsbSvcConfig, "subcmd", []string{"sss"}, "test")
		assert.Equal(t, 1, len(j.Spec.Template.Spec.Containers))
		assert.True(t, len(j.Spec.Template.Spec.Containers[0].Args) > 0)
		assert.Contains(t, j.Name, testPipeline.Name+"-buffer-test-")
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
		activeDeadlineSeconds := int64(600)
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
				ActiveDeadlineSeconds: &activeDeadlineSeconds,
				BackoffLimit:          &backoffLimit,
				ContainerTemplate: &dfv1.ContainerTemplate{
					Resources: resources,
					Env:       []corev1.EnvVar{env},
				},
				Metadata: &dfv1.Metadata{
					Annotations: podAnnotations,
					Labels:      podLabels,
				},
				NodeSelector:      nodeSelector,
				Tolerations:       []corev1.Toleration{toleration},
				PriorityClassName: "my-priority-class-name",
				Priority:          &priority,
			},
		}
		j := buildISBBatchJob(pl, testFlowImage, fakeIsbSvcConfig, "subcmd", []string{"sss"}, "test")
		assert.Equal(t, 1, len(j.Spec.Template.Spec.Containers))
		assert.Equal(t, j.Spec.Template.Spec.Containers[0].Resources, resources)
		assert.Greater(t, len(j.Spec.Template.Spec.Containers[0].Env), 1)
		assert.Contains(t, j.Spec.Template.Spec.Containers[0].Env, env)
		assert.Equal(t, j.Spec.Template.Labels["my-label-name"], podLabels["my-label-name"])
		assert.Equal(t, j.Spec.Template.Annotations["my-annotation-name"], podAnnotations["my-annotation-name"])
		assert.NotNil(t, j.Spec.ActiveDeadlineSeconds)
		assert.Equal(t, *j.Spec.ActiveDeadlineSeconds, activeDeadlineSeconds)
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
		config: fakeConfig,
		image:  testFlowImage,
		logger: zaptest.NewLogger(t).Sugar(),
	}

	t.Run("test create cleanup buffer job no isbsvc", func(t *testing.T) {
		testObj := testPipeline.DeepCopy()
		assert.Equal(t, 4, len(testObj.GetAllBuffers()))
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
		assert.Contains(t, jobs.Items[0].Name, "cleanup")
		assert.Equal(t, 0, len(jobs.Items[0].OwnerReferences))
	})
}

func TestCreateOrUpdateDaemon(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()
	r := &pipelineReconciler{
		client: cl,
		scheme: scheme.Scheme,
		config: fakeConfig,
		image:  testFlowImage,
		logger: zaptest.NewLogger(t).Sugar(),
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
