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

package isbsvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
)

const (
	testISBSvcName      = "test-isb-svc"
	testNamespace       = "test-ns"
	testVersion         = "6.2.6"
	testImage           = "test-image"
	testSImage          = "test-s-image"
	testJSImage         = "test-nats-image"
	testJSReloaderImage = "test-nats-rl-image"
	testJSMetricsImage  = "test-nats-m-image"
)

var (
	fakeGlobalISBSvcConfig = &reconciler.ISBSvcConfig{
		JetStream: &reconciler.JetStreamConfig{
			Versions: []reconciler.JetStreamVersion{
				{
					Version:              testVersion,
					NatsImage:            testJSImage,
					ConfigReloaderImage:  testJSReloaderImage,
					MetricsExporterImage: testJSMetricsImage,
				},
			},
		},
	}

	jetStreamIsbs = &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testISBSvcName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{
			JetStream: &dfv1.JetStreamBufferService{
				Version: testVersion,
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
	kubeClient := k8sfake.NewSimpleClientset()
	r := NewReconciler(cl, kubeClient, scheme.Scheme, reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig), zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
	_, ok := r.(*interStepBufferServiceReconciler)
	assert.True(t, ok)
}

func TestReconcileJetStream(t *testing.T) {
	t.Run("jetstream isbs installation", func(t *testing.T) {
		testIsb := jetStreamIsbs.DeepCopy()
		ctx := context.TODO()
		cl := fake.NewClientBuilder().Build()
		r := &interStepBufferServiceReconciler{
			client:     cl,
			kubeClient: k8sfake.NewSimpleClientset(),
			scheme:     scheme.Scheme,
			config:     reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			logger:     zaptest.NewLogger(t).Sugar(),
			recorder:   record.NewFakeRecorder(64),
		}
		err := r.reconcile(ctx, testIsb)
		assert.NoError(t, err)
		testIsb.Status.MarkChildrenResourceHealthy("RolloutFinished", "All service healthy")
		assert.True(t, testIsb.Status.IsHealthy())
		assert.NotNil(t, testIsb.Status.Config.JetStream)
		assert.NotEmpty(t, testIsb.Status.Config.JetStream.URL)
		assert.NotNil(t, testIsb.Status.Config.JetStream.Auth)
		assert.NotNil(t, testIsb.Status.Config.JetStream.Auth.Basic)
		assert.NotNil(t, testIsb.Status.Config.JetStream.Auth.Basic.User)
		assert.NotNil(t, testIsb.Status.Config.JetStream.Auth.Basic.Password)
		sts := &appv1.StatefulSetList{}
		selector, _ := labels.Parse(dfv1.KeyComponent + "=" + "isbsvc")
		err = r.client.List(ctx, sts, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sts.Items))
		secrets := &corev1.SecretList{}
		err = r.client.List(ctx, secrets, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(secrets.Items))
		svcs := &corev1.ServiceList{}
		err = r.client.List(ctx, svcs, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(svcs.Items))
		cms := &corev1.ConfigMapList{}
		err = r.client.List(ctx, cms, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(cms.Items))
	})
}

func TestNeedsUpdate(t *testing.T) {

	t.Run("needs jetstream update", func(t *testing.T) {
		testIsbs := jetStreamIsbs.DeepCopy()
		controllerutil.AddFinalizer(testIsbs, finalizerName)
		assert.True(t, contains(testIsbs.Finalizers, finalizerName))
		controllerutil.RemoveFinalizer(testIsbs, finalizerName)
		assert.False(t, contains(testIsbs.Finalizers, finalizerName))
	})
}

func contains(arr []string, str string) bool {
	for _, a := range arr {
		if a == str {
			return true
		}
	}
	return false
}
