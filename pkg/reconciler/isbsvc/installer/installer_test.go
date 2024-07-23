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

package installer

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
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const (
	testISBSName           = "test-isb"
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
	testLabels = map[string]string{"a": "b"}

	testNativeRedisIsbSvc = &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testISBSName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{
			Redis: &dfv1.RedisBufferService{
				Native: &dfv1.NativeRedis{
					Version: testVersion,
				},
			},
		},
	}

	testJetStreamIsbSvc = &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testISBSName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{
			JetStream: &dfv1.JetStreamBufferService{
				Version: testVersion,
			},
		},
	}

	testExternalRedisIsbSvc = &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      testISBSName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{

			Redis: &dfv1.RedisBufferService{
				External: &dfv1.RedisConfig{
					URL: "xxxxx",
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
)

func init() {
	_ = dfv1.AddToScheme(scheme.Scheme)
	_ = appv1.AddToScheme(scheme.Scheme)
	_ = corev1.AddToScheme(scheme.Scheme)
}

func TestGetInstaller(t *testing.T) {

	fakeConfig := reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig)

	t.Run("get native redis installer", func(t *testing.T) {
		installer, err := getInstaller(testNativeRedisIsbSvc, nil, nil, fakeConfig, zaptest.NewLogger(t).Sugar(), nil)
		assert.NoError(t, err)
		assert.NotNil(t, installer)
		_, ok := installer.(*redisInstaller)
		assert.True(t, ok)
	})

	t.Run("get jetstream installer", func(t *testing.T) {
		installer, err := getInstaller(testJetStreamIsbSvc, nil, nil, fakeConfig, zaptest.NewLogger(t).Sugar(), nil)
		assert.NoError(t, err)
		assert.NotNil(t, installer)
		_, ok := installer.(*jetStreamInstaller)
		assert.True(t, ok)
	})

	t.Run("get external redis installer", func(t *testing.T) {
		installer, err := getInstaller(testExternalRedisIsbSvc, nil, nil, fakeConfig, zaptest.NewLogger(t).Sugar(), nil)
		assert.NoError(t, err)
		assert.NotNil(t, installer)
		_, ok := installer.(*externalRedisInstaller)
		assert.True(t, ok)
	})

	t.Run("test error", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		testObj.Spec.Redis = nil
		_, err := getInstaller(testObj, nil, nil, fakeConfig, zaptest.NewLogger(t).Sugar(), nil)
		assert.Error(t, err)
	})
}

func TestInstall(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	kubeClient := k8sfake.NewSimpleClientset()
	fakeConfig := reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig)
	ctx := context.TODO()
	t.Run("test redis error", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		testObj.Spec.Redis = nil
		err := Install(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.Error(t, err)
		assert.Equal(t, "invalid isb service spec", err.Error())
	})

	t.Run("test redis install ok", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		err := Install(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.NoError(t, err)
		assert.True(t, testObj.Status.IsReady())
		assert.True(t, testObj.Status.IsHealthy())
		assert.NotNil(t, testObj.Status.Config.Redis)
		assert.NotEmpty(t, testObj.Status.Config.Redis.SentinelURL)
		assert.NotEmpty(t, testObj.Status.Config.Redis.MasterName)
		assert.NotEmpty(t, testObj.Status.Config.Redis.User)
		assert.NotNil(t, testObj.Status.Config.Redis.Password)
	})

	t.Run("test jetstream error", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		testObj.Spec.JetStream = nil
		err := Install(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.Error(t, err)
		assert.Equal(t, "invalid isb service spec", err.Error())
	})

	t.Run("test jetstream install ok", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		err := Install(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.NoError(t, err)
		assert.True(t, testObj.Status.IsReady())
		assert.True(t, testObj.Status.IsHealthy())
		assert.NotNil(t, testObj.Status.Config.JetStream)
		assert.NotEmpty(t, testObj.Status.Config.JetStream.StreamConfig)
		assert.NotEmpty(t, testObj.Status.Config.JetStream.URL)
		assert.NotNil(t, testObj.Status.Config.JetStream.Auth)
		assert.NotNil(t, testObj.Status.Config.JetStream.Auth.Basic)
		assert.NotNil(t, testObj.Status.Config.JetStream.Auth.Basic.User)
		assert.NotNil(t, testObj.Status.Config.JetStream.Auth.Basic.Password)
	})
}

func TestUnInstall(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	kubeClient := k8sfake.NewSimpleClientset()
	fakeConfig := reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig)
	ctx := context.TODO()
	t.Run("test redis error", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		testObj.Spec.Redis = nil
		err := Uninstall(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.Error(t, err)
		assert.Equal(t, "invalid isb service spec", err.Error())
	})

	t.Run("test redis uninstall ok", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		err := Uninstall(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.NoError(t, err)
	})

	t.Run("test jetstream error", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		testObj.Spec.JetStream = nil
		err := Uninstall(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.Error(t, err)
		assert.Equal(t, "invalid isb service spec", err.Error())
	})

	t.Run("test jetstream uninstall ok", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		err := Uninstall(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.NoError(t, err)
	})
}
