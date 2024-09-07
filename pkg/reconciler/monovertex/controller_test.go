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

	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/numaproj/numaflow/pkg/reconciler/monovertex/scaling"
	"github.com/stretchr/testify/assert"
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

func Test_BuildPodSpec(t *testing.T) {
	fakeConfig := reconciler.FakeGlobalConfig(t, nil)
	cl := fake.NewClientBuilder().Build()
	r := &monoVertexReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   fakeConfig,
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
	}
	t.Run("test has everything", func(t *testing.T) {
		testObj := testMonoVtx.DeepCopy()
		spec, err := r.buildPodSpec(testObj)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(spec.Containers))
		assert.Equal(t, dfv1.CtrMain, spec.Containers[0].Name)
		assert.Equal(t, dfv1.CtrUdsource, spec.Containers[1].Name)
		assert.Equal(t, dfv1.CtrUdtransformer, spec.Containers[2].Name)
		assert.Equal(t, dfv1.CtrUdsink, spec.Containers[3].Name)
		assert.Equal(t, dfv1.CtrFallbackUdsink, spec.Containers[4].Name)
		assert.Equal(t, 0, len(spec.InitContainers))
	})

	t.Run("test no transformer, no fallback sink", func(t *testing.T) {
		testObj := testMonoVtx.DeepCopy()
		testObj.Spec.Source.UDTransformer = nil
		testObj.Spec.Sink.Fallback = nil
		spec, err := r.buildPodSpec(testObj)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(spec.Containers))
		assert.Equal(t, dfv1.CtrMain, spec.Containers[0].Name)
		assert.Equal(t, dfv1.CtrUdsource, spec.Containers[1].Name)
		assert.Equal(t, dfv1.CtrUdsink, spec.Containers[2].Name)
		assert.Equal(t, 0, len(spec.InitContainers))
	})
}

func Test_createOrUpdateDaemonDeployment(t *testing.T) {
	fakeConfig := reconciler.FakeGlobalConfig(t, nil)
	cl := fake.NewClientBuilder().Build()
	r := &monoVertexReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   fakeConfig,
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
	}

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
	fakeConfig := reconciler.FakeGlobalConfig(t, nil)
	cl := fake.NewClientBuilder().Build()
	r := &monoVertexReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   fakeConfig,
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
	}

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
	fakeConfig := reconciler.FakeGlobalConfig(t, nil)
	cl := fake.NewClientBuilder().Build()
	r := &monoVertexReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   fakeConfig,
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
	}

	t.Run("test everything from scratch for monovtx service", func(t *testing.T) {
		testObj := testMonoVtx.DeepCopy()
		err := r.createOrUpdateMonoVtxServices(context.TODO(), testObj)
		assert.NoError(t, err)
		var svc corev1.Service
		err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testObj.GetNamespace(), Name: testObj.GetHeadlessServiceName()},
			&svc)
		assert.NoError(t, err)
		assert.Equal(t, testObj.GetHeadlessServiceName(), svc.Name)
		assert.Equal(t, 1, len(svc.Spec.Ports))
		assert.Equal(t, int32(dfv1.MonoVertexMetricsPort), svc.Spec.Ports[0].Port)
		m, err := r.findExistingMonoVtxServices(context.TODO(), testObj)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(m))
	})
}

func Test_orchestratePods(t *testing.T) {
	fakeConfig := reconciler.FakeGlobalConfig(t, nil)
	cl := fake.NewClientBuilder().Build()
	r := &monoVertexReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   fakeConfig,
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
	}
	t.Run("test orchestratePodsFromTo and cleanUpPodsFromTo", func(t *testing.T) {
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
	fakeConfig := reconciler.FakeGlobalConfig(t, nil)
	cl := fake.NewClientBuilder().Build()
	r := &monoVertexReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   fakeConfig,
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
	}
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
	fakeConfig := reconciler.FakeGlobalConfig(t, nil)
	cl := fake.NewClientBuilder().Build()
	r := &monoVertexReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   fakeConfig,
		image:    testFlowImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
		scaler:   scaling.NewScaler(cl),
	}
	testObj := testMonoVtx.DeepCopy()
	_, err := r.reconcile(context.TODO(), testObj)
	assert.NoError(t, err)
	var daemonDeployment appv1.Deployment
	err = r.client.Get(context.TODO(), client.ObjectKey{Namespace: testObj.GetNamespace(), Name: testObj.GetDaemonDeploymentName()},
		&daemonDeployment)
	assert.NoError(t, err)
	assert.Equal(t, testObj.GetDaemonDeploymentName(), daemonDeployment.Name)
}
