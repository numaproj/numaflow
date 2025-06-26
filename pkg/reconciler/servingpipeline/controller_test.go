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

package servingpipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
)

const (
	testNamespace = "test-ns"
	testImage     = "test-image"
)

var (
	testServingPipeline = &dfv1.ServingPipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-spl",
			Namespace: testNamespace,
		},
		Spec: dfv1.ServingPipelineSpec{
			Pipeline: dfv1.PipelineSpec{
				Vertices: []dfv1.AbstractVertex{
					{
						Name: "input",
						Source: &dfv1.Source{
							UDTransformer: &dfv1.UDTransformer{
								Container: &dfv1.Container{Image: "test-ud-transformer-image"},
							},
						},
					},
					{
						Name: "output",
						Sink: &dfv1.Sink{},
					},
				},
				Edges: []dfv1.Edge{
					{From: "input", To: "output"},
				},
			},
			Serving: dfv1.ServingSpec{
				Replicas: ptr.To[int32](5),
			},
		},
	}
)

func init() {
	_ = dfv1.AddToScheme(scheme.Scheme)
	_ = appv1.AddToScheme(scheme.Scheme)
	_ = corev1.AddToScheme(scheme.Scheme)
	_ = batchv1.AddToScheme(scheme.Scheme)
	_ = autoscalingv2.AddToScheme(scheme.Scheme)
}

func fakeReconciler(t *testing.T, cl client.WithWatch) *servingPipelineReconciler {
	t.Helper()
	return &servingPipelineReconciler{
		client:   cl,
		scheme:   scheme.Scheme,
		config:   reconciler.FakeGlobalConfig(t, nil),
		image:    testImage,
		logger:   zaptest.NewLogger(t).Sugar(),
		recorder: record.NewFakeRecorder(64),
	}
}

func TestReconcile(t *testing.T) {
	t.Run("Reconciling a non-existent ServingPipeline", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		r := fakeReconciler(t, cl)
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      "not-exist",
				Namespace: testNamespace,
			},
		}
		_, err := r.Reconcile(context.TODO(), req)
		assert.NoError(t, err)
	})

	t.Run("Reconcile a valid ServingPipeline", func(t *testing.T) {
		cl := fake.NewClientBuilder().Build()
		ctx := context.TODO()
		r := fakeReconciler(t, cl)

		testIsbSvc := &dfv1.InterStepBufferService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      dfv1.DefaultISBSvcName,
				Namespace: testNamespace,
			},
			Status: dfv1.InterStepBufferServiceStatus{
				Config: dfv1.BufferServiceConfig{
					JetStream: &dfv1.JetStreamConfig{
						URL: "nats://test",
					},
				},
			},
		}
		testIsbSvc.Status.MarkConfigured()
		testIsbSvc.Status.MarkDeployed()
		err := cl.Create(ctx, testIsbSvc)
		require.NoError(t, err)

		testObj := testServingPipeline.DeepCopy()
		err = r.reconcileFixedResources(ctx, testObj)
		require.NoError(t, err)
		req := ctrl.Request{
			NamespacedName: types.NamespacedName{
				Name:      testObj.Name,
				Namespace: testNamespace,
			},
		}
		_, err = r.Reconcile(context.TODO(), req)
		require.NoError(t, err)

		serverDeploy := &appv1.Deployment{}
		err = cl.Get(context.TODO(), types.NamespacedName{
			Namespace: testServingPipeline.Namespace,
			Name:      testServingPipeline.GetServingServerName(),
		}, serverDeploy)
		require.NoError(t, err)
		require.Equal(t, int32(5), *serverDeploy.Spec.Replicas)
	})
}

func Test_CleanUpJobCreation(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()
	r := fakeReconciler(t, cl)

	testIsbSvc := &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dfv1.DefaultISBSvcName,
			Namespace: testNamespace,
		},
		Status: dfv1.InterStepBufferServiceStatus{
			Config: dfv1.BufferServiceConfig{
				JetStream: &dfv1.JetStreamConfig{
					URL: "nats://test",
				},
			},
		},
	}
	testIsbSvc.Status.MarkConfigured()
	testIsbSvc.Status.MarkDeployed()
	err := cl.Create(ctx, testIsbSvc)
	require.NoError(t, err)

	testObj := testServingPipeline.DeepCopy()
	err = r.cleanUp(ctx, testObj, r.logger)
	require.NoError(t, err)

	jobs := &batchv1.JobList{}
	err = cl.List(ctx, jobs, &client.ListOptions{Namespace: testNamespace})
	require.NoError(t, err)
	require.Equal(t, 1, len(jobs.Items))
	require.Contains(t, jobs.Items[0].Name, "cln")
}
