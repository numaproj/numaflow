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

package monovertex

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/reconciler/monovertex/scaling"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

const (
	testNamespace   = "test-ns"
	testMonoVtxName = "tmvtx"
	testFlowImage   = "test-d-iamge"
)

var (
	testMonoVtx = &dfv1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testMonoVtxName,
		},
		Spec: dfv1.MonoVertexSpec{
			Scale: dfv1.Scale{
				Min: ptr.To[int32](2),
			},
			Source: &dfv1.Source{
				UDSource: &dfv1.UDSource{
					Container: &dfv1.Container{
						Image: "test-source-image",
					},
				},
				UDTransformer: &dfv1.UDTransformer{
					Container: &dfv1.Container{
						Image: "test-tf-image",
					},
				},
			},
			Sink: &dfv1.Sink{
				AbstractSink: dfv1.AbstractSink{
					UDSink: &dfv1.UDSink{
						Container: &dfv1.Container{
							Image: "test-sink",
						},
					},
				},
				Fallback: &dfv1.AbstractSink{
					UDSink: &dfv1.UDSink{
						Container: &dfv1.Container{
							Image: "test-fb-sink",
						},
					},
				},
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
	r := NewReconciler(cl, scheme.Scheme, reconciler.FakeGlobalConfig(t, nil), testFlowImage, scaling.NewScaler(cl), zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
	_, ok := r.(*monoVertexReconciler)
	assert.True(t, ok)
}

func fakeReconciler(t *testing.T, cl client.WithWatch) *monoVertexReconciler {
	t.Helper()
	return &monoVertexReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   reconciler.FakeGlobalConfig(t, nil),
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
		scaler:   scaling.NewScaler(cl),
	}
}

func Test_pauseAndResumeMvtx(t *testing.T) {

	t.Run("test pause mvtx", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)
		testObj := testMonoVtx.DeepCopy()
		testObj.Spec.Replicas = ptr.To[int32](2)
		err := cl.Create(context.TODO(), testObj)
		assert.NoError(t, err)

		_, err = r.reconcile(context.TODO(), testObj)
		assert.NoError(t, err)
		mvtx := &dfv1.MonoVertex{}
		err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testNamespace, Name: testObj.Name}, mvtx)
		assert.NoError(t, err)
		assert.Equal(t, *mvtx.Spec.Replicas, int32(2))

		testObj.Spec.Lifecycle.DesiredPhase = dfv1.MonoVertexPhasePaused
		_, err = r.reconcile(context.TODO(), testObj)
		assert.NoError(t, err)
	})
}

func TestReconcile(t *testing.T) {
	t.Run("test not found", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "not-exist",
				Namespace: testNamespace,
			},
		}
		_, err := r.Reconcile(context.TODO(), req)
		// Return nil when not found
		assert.NoError(t, err)
	})

	t.Run("test found", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)
		testObj := testMonoVtx.DeepCopy()
		err := cl.Create(context.TODO(), testObj)
		assert.NoError(t, err)
		o := &dfv1.MonoVertex{}
		err = cl.Get(context.TODO(), types.NamespacedName{
			Namespace: testObj.Namespace,
			Name:      testObj.Name,
		}, o)
		assert.NoError(t, err)
		assert.Equal(t, testObj.Name, o.Name)
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      testObj.Name,
				Namespace: testObj.Namespace,
			},
		}
		_, err = r.Reconcile(context.TODO(), req)
		assert.Error(t, err)
	})
}

func Test_BuildPodSpec(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	r := fakeReconciler(t, cl)
	t.Run("test has everything", func(t *testing.T) {
		testObj := testMonoVtx.DeepCopy()
		spec, err := r.buildPodSpec(testObj)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.Containers))
		assert.Equal(t, dfv1.CtrMain, spec.Containers[0].Name)
		assert.Equal(t, 5, len(spec.InitContainers))
		assert.Equal(t, dfv1.CtrMonitor, spec.InitContainers[0].Name)
		assert.Equal(t, dfv1.CtrUdsource, spec.InitContainers[1].Name)
		assert.Equal(t, dfv1.CtrUdtransformer, spec.InitContainers[2].Name)
		assert.Equal(t, dfv1.CtrUdsink, spec.InitContainers[3].Name)
		assert.Equal(t, dfv1.CtrFallbackUdsink, spec.InitContainers[4].Name)
	})

	t.Run("test no transformer, no fallback sink", func(t *testing.T) {
		testObj := testMonoVtx.DeepCopy()
		testObj.Spec.Source.UDTransformer = nil
		testObj.Spec.Sink.Fallback = nil
		spec, err := r.buildPodSpec(testObj)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(spec.Containers))
		assert.Equal(t, dfv1.CtrMain, spec.Containers[0].Name)
		assert.Equal(t, 3, len(spec.InitContainers))
		assert.Equal(t, dfv1.CtrMonitor, spec.InitContainers[0].Name)
		assert.Equal(t, dfv1.CtrUdsource, spec.InitContainers[1].Name)
		assert.Equal(t, dfv1.CtrUdsink, spec.InitContainers[2].Name)
	})
}

func Test_createOrUpdateDaemonDeployment(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	r := fakeReconciler(t, cl)

	t.Run("test everything from scratch for daemon deployment", func(t *testing.T) {
		testObj := testMonoVtx.DeepCopy()
		err := r.createOrUpdateDaemonDeployment(context.TODO(), testObj)
		assert.NoError(t, err)
		var daemonDeployment appv1.Deployment
		err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testObj.GetNamespace(), Name: testObj.GetDaemonDeploymentName()},
			&daemonDeployment)
		assert.NoError(t, err)
		assert.Equal(t, testObj.GetDaemonDeploymentName(), daemonDeployment.Name)
		assert.Equal(t, 1, len(daemonDeployment.Spec.Template.Spec.Containers))
		assert.Equal(t, dfv1.CtrMain, daemonDeployment.Spec.Template.Spec.Containers[0].Name)
	})
}

func Test_createOrUpdateDaemonService(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	r := fakeReconciler(t, cl)

	t.Run("test everything from scratch for daemon service", func(t *testing.T) {
		testObj := testMonoVtx.DeepCopy()
		err := r.createOrUpdateDaemonService(context.TODO(), testObj)
		assert.NoError(t, err)
		var daemonSvc corev1.Service
		err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testObj.GetNamespace(), Name: testObj.GetDaemonServiceName()},
			&daemonSvc)
		assert.NoError(t, err)
		assert.Equal(t, testObj.GetDaemonServiceName(), daemonSvc.Name)
		assert.Equal(t, 1, len(daemonSvc.Spec.Ports))
		assert.Equal(t, int32(dfv1.MonoVertexDaemonServicePort), daemonSvc.Spec.Ports[0].Port)
	})
}

func Test_createOrUpdateMonoVtxServices(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	r := fakeReconciler(t, cl)

	t.Run("test everything from scratch for monovtx service", func(t *testing.T) {
		testObj := testMonoVtx.DeepCopy()
		err := r.createOrUpdateMonoVtxServices(context.TODO(), testObj)
		assert.NoError(t, err)
		var svc corev1.Service
		err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testObj.GetNamespace(), Name: testObj.GetHeadlessServiceName()},
			&svc)
		assert.NoError(t, err)
		assert.Equal(t, testObj.GetHeadlessServiceName(), svc.Name)
		assert.Equal(t, 2, len(svc.Spec.Ports))
		m, err := r.findExistingMonoVtxServices(context.TODO(), testObj)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(m))
	})
}

func Test_orchestratePods(t *testing.T) {

	t.Run("test orchestratePodsFromTo and cleanUpPodsFromTo", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)
		testObj := testMonoVtx.DeepCopy()
		hash := "test-hasssssh"
		podSpec, err := r.buildPodSpec(testObj)
		assert.NoError(t, err)
		err = r.orchestratePodsFromTo(context.TODO(), testObj, *podSpec, 2, 4, hash)
		assert.NoError(t, err)
		foundPods, err := r.findExistingPods(context.TODO(), testObj, 2, 4)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(foundPods))
		for n, pod := range foundPods {
			assert.Equal(t, hash, pod.Annotations[dfv1.KeyHash])
			assert.True(t, strings.HasPrefix(n, testObj.Name+"-mv-2") || strings.HasPrefix(n, testObj.Name+"-mv-3"))
		}
		err = r.cleanUpPodsFromTo(context.TODO(), testObj, 2, 4)
		assert.NoError(t, err)
		foundPods, err = r.findExistingPods(context.TODO(), testObj, 2, 4)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(foundPods))
	})

	t.Run("test orchestratePods", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)
		testObj := testMonoVtx.DeepCopy()
		err := r.orchestratePods(context.TODO(), testObj)
		assert.NoError(t, err)
		foundPods, err := r.findExistingPods(context.TODO(), testObj, 0, 4)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(foundPods))
		for n := range foundPods {
			assert.True(t, strings.HasPrefix(n, testObj.Name+"-mv-0") || strings.HasPrefix(n, testObj.Name+"-mv-1"))
		}
	})
}

func Test_orchestrateFixedResources(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	r := fakeReconciler(t, cl)
	testObj := testMonoVtx.DeepCopy()
	err := r.orchestrateFixedResources(context.TODO(), testObj)
	assert.NoError(t, err)
	var svc corev1.Service
	err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testObj.GetNamespace(), Name: testObj.GetHeadlessServiceName()},
		&svc)
	assert.NoError(t, err)
	assert.Equal(t, testObj.GetHeadlessServiceName(), svc.Name)
	var daemonSvc corev1.Service
	err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testObj.GetNamespace(), Name: testObj.GetDaemonServiceName()},
		&daemonSvc)
	assert.NoError(t, err)
	assert.Equal(t, testObj.GetDaemonServiceName(), daemonSvc.Name)
	var daemonDeployment appv1.Deployment
	err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testObj.GetNamespace(), Name: testObj.GetDaemonDeploymentName()},
		&daemonDeployment)
	assert.NoError(t, err)
	assert.Equal(t, testObj.GetDaemonDeploymentName(), daemonDeployment.Name)
}

func Test_reconcile(t *testing.T) {

	t.Run("test deletion", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)
		testObj := testMonoVtx.DeepCopy()
		testObj.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		_, err := r.reconcile(context.TODO(), testObj)
		assert.NoError(t, err)
	})

	t.Run("test okay", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)
		testObj := testMonoVtx.DeepCopy()
		_, err := r.reconcile(context.TODO(), testObj)
		assert.NoError(t, err)
		var daemonDeployment appv1.Deployment
		err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testObj.GetNamespace(), Name: testObj.GetDaemonDeploymentName()},
			&daemonDeployment)
		assert.NoError(t, err)
		assert.Equal(t, testObj.GetDaemonDeploymentName(), daemonDeployment.Name)
	})

	t.Run("test reconcile rolling update", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)
		testObj := testMonoVtx.DeepCopy()
		testObj.Spec.Replicas = ptr.To[int32](3)
		_, err := r.reconcile(context.TODO(), testObj)
		assert.NoError(t, err)
		pods := &corev1.PodList{}
		selector, _ := labels.Parse(dfv1.KeyComponent + "=" + dfv1.ComponentMonoVertex + "," + dfv1.KeyMonoVertexName + "=" + testObj.Name)
		err = r.client.List(context.TODO(), pods, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(pods.Items))

		podSpec, _ := r.buildPodSpec(testObj)
		hash := sharedutil.MustHash(podSpec)
		testObj.Status.Replicas = 3
		testObj.Status.ReadyReplicas = 3
		testObj.Status.UpdateHash = hash
		testObj.Status.CurrentHash = hash

		// Reduce desired replicas
		testObj.Spec.Replicas = ptr.To[int32](2)
		_, err = r.reconcile(context.TODO(), testObj)
		assert.NoError(t, err)
		err = r.client.List(context.TODO(), pods, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(pods.Items))
		assert.Equal(t, uint32(2), testObj.Status.Replicas)
		assert.Equal(t, uint32(2), testObj.Status.UpdatedReplicas)

		// updatedReplicas > desiredReplicas
		testObj.Status.UpdatedReplicas = 3
		_, err = r.reconcile(context.TODO(), testObj)
		assert.NoError(t, err)
		assert.Equal(t, uint32(2), testObj.Status.UpdatedReplicas)

		// Clean up
		testObj.Spec.Replicas = ptr.To[int32](0)
		testObj.Spec.Scale.Min = ptr.To[int32](0)
		_, err = r.reconcile(context.TODO(), testObj)
		assert.NoError(t, err)
		err = r.client.List(context.TODO(), pods, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(pods.Items))

		// rolling update
		testObj.Spec.Replicas = ptr.To[int32](20)
		testObj.Status.UpdatedReplicas = 20
		testObj.Status.UpdatedReadyReplicas = 20
		testObj.Status.Replicas = 20
		testObj.Status.CurrentHash = "123456"
		testObj.Status.UpdateHash = "123456"
		_, err = r.reconcile(context.TODO(), testObj)
		assert.NoError(t, err)
		err = r.client.List(context.TODO(), pods, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 5, len(pods.Items))
		assert.Equal(t, uint32(20), testObj.Status.Replicas)
		assert.Equal(t, uint32(5), testObj.Status.UpdatedReplicas)
	})

	t.Run("test isTerminatingPod", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)

		now := metav1.Now()
		// Pod with DeletionTimestamp set
		pod1 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pod1",
				Namespace:         testNamespace,
				DeletionTimestamp: &now,
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		}
		assert.True(t, r.isTerminatingPod(pod1), "Pod with DeletionTimestamp should be terminating")

		// Pod in Failed phase
		pod2 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod2",
				Namespace: testNamespace,
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
			},
		}
		assert.True(t, r.isTerminatingPod(pod2), "Pod in Failed phase should be terminating")

		// Pod in Succeeded phase
		pod3 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod3",
				Namespace: testNamespace,
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodSucceeded,
			},
		}
		assert.True(t, r.isTerminatingPod(pod3), "Pod in Succeeded phase should be terminating")

		// Pod in Running phase, no DeletionTimestamp
		pod4 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod4",
				Namespace: testNamespace,
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		}
		assert.False(t, r.isTerminatingPod(pod4), "Pod in Running phase with no DeletionTimestamp should not be terminating")
	})
}
