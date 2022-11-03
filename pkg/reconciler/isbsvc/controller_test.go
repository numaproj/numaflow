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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reconciler"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	testISBSvcName         = "test-isb-svc"
	testNamespace          = "test-ns"
	testVersion            = "6.2.6"
	testImage              = "test-image"
	testSImage             = "test-s-image"
	testRedisExporterImage = "test-r-exporter-image"
	testJSImage            = "test-nats-image"
	testJSReloaderImage    = "test-nats-rl-image"
	testJSMetricsImage     = "test-nats-m-image"
)

var (
	nativeRedisIsbs = &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testISBSvcName,
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
	r := NewReconciler(cl, scheme.Scheme, fakeConfig, zaptest.NewLogger(t).Sugar())
	_, ok := r.(*interStepBufferServiceReconciler)
	assert.True(t, ok)
}

func TestReconcileNativeRedis(t *testing.T) {
	t.Run("native redis isb svc installation", func(t *testing.T) {
		testIsb := nativeRedisIsbs.DeepCopy()
		ctx := context.TODO()
		cl := fake.NewClientBuilder().Build()
		r := &interStepBufferServiceReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		err := r.reconcile(ctx, testIsb)
		assert.NoError(t, err)
		assert.True(t, testIsb.Status.IsReady())
		assert.NotNil(t, testIsb.Status.Config.Redis)
		assert.NotEmpty(t, testIsb.Status.Config.Redis.SentinelURL)
		assert.NotEmpty(t, testIsb.Status.Config.Redis.User)
		assert.NotNil(t, testIsb.Status.Config.Redis.SentinelPassword)
		assert.NotNil(t, testIsb.Status.Config.Redis.Password)
		sts := &appv1.StatefulSetList{}
		selector, _ := labels.Parse(dfv1.KeyComponent + "=" + "isbsvc")
		err = r.client.List(ctx, sts, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sts.Items))
		secrets := &corev1.SecretList{}
		err = r.client.List(ctx, secrets, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(secrets.Items))
		svcs := &corev1.ServiceList{}
		err = r.client.List(ctx, svcs, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 2, len(svcs.Items))
		cms := &corev1.ConfigMapList{}
		err = r.client.List(ctx, cms, &client.ListOptions{Namespace: testNamespace, LabelSelector: selector})
		assert.NoError(t, err)
		assert.Equal(t, 3, len(cms.Items))
	})
}

func TestReconcileJetStream(t *testing.T) {
	t.Run("jetstream isbs installation", func(t *testing.T) {
		testIsb := jetStreamIsbs.DeepCopy()
		ctx := context.TODO()
		cl := fake.NewClientBuilder().Build()
		r := &interStepBufferServiceReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		err := r.reconcile(ctx, testIsb)
		assert.NoError(t, err)
		assert.True(t, testIsb.Status.IsReady())
		assert.NotNil(t, testIsb.Status.Config.JetStream)
		assert.NotEmpty(t, testIsb.Status.Config.JetStream.URL)
		assert.NotEmpty(t, testIsb.Status.Config.JetStream.Auth)
		assert.NotNil(t, testIsb.Status.Config.JetStream.Auth.User)
		assert.NotNil(t, testIsb.Status.Config.JetStream.Auth.Password)
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
	t.Run("needs redis update", func(t *testing.T) {
		testIsbs := nativeRedisIsbs.DeepCopy()
		cl := fake.NewClientBuilder().Build()
		r := &interStepBufferServiceReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		assert.False(t, r.needsUpdate(nativeRedisIsbs, testIsbs))
		controllerutil.AddFinalizer(testIsbs, finalizerName)
		assert.True(t, contains(testIsbs.Finalizers, finalizerName))
		assert.True(t, r.needsUpdate(nativeRedisIsbs, testIsbs))
		controllerutil.RemoveFinalizer(testIsbs, finalizerName)
		assert.False(t, contains(testIsbs.Finalizers, finalizerName))
		assert.False(t, r.needsUpdate(nativeRedisIsbs, testIsbs))
		testIsbs.Status.MarkConfigured()
		assert.False(t, r.needsUpdate(nativeRedisIsbs, testIsbs))
	})

	t.Run("needs jetstream update", func(t *testing.T) {
		testIsbs := jetStreamIsbs.DeepCopy()
		cl := fake.NewClientBuilder().Build()
		r := &interStepBufferServiceReconciler{
			client: cl,
			scheme: scheme.Scheme,
			config: fakeConfig,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		assert.False(t, r.needsUpdate(nativeRedisIsbs, testIsbs))
		controllerutil.AddFinalizer(testIsbs, finalizerName)
		assert.True(t, contains(testIsbs.Finalizers, finalizerName))
		assert.True(t, r.needsUpdate(nativeRedisIsbs, testIsbs))
		controllerutil.RemoveFinalizer(testIsbs, finalizerName)
		assert.False(t, contains(testIsbs.Finalizers, finalizerName))
		assert.False(t, r.needsUpdate(nativeRedisIsbs, testIsbs))
		testIsbs.Status.MarkConfigured()
		assert.False(t, r.needsUpdate(nativeRedisIsbs, testIsbs))
	})
}

func TestNeedsFinalizer(t *testing.T) {
	t.Run("needs finalizer redis", func(t *testing.T) {
		testStorageClass := "test"
		testIsbs := nativeRedisIsbs.DeepCopy()
		testIsbs.Spec.Redis.Native.Persistence = &dfv1.PersistenceStrategy{
			StorageClassName: &testStorageClass,
		}
		assert.True(t, needsFinalizer(testIsbs))
	})

	t.Run("needs finalizer jetstream", func(t *testing.T) {
		testStorageClass := "test"
		testIsbs := testJetStreamIsbs.DeepCopy()
		testIsbs.Spec.JetStream.Persistence = &dfv1.PersistenceStrategy{
			StorageClassName: &testStorageClass,
		}
		assert.True(t, needsFinalizer(testIsbs))
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
