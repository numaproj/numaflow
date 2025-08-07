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
	testISBSName        = "test-isb"
	testNamespace       = "test-ns"
	testVersion         = "6.2.6"
	testJSImage         = "test-nats-image"
	testJSReloaderImage = "test-nats-rl-image"
	testJSMetricsImage  = "test-nats-m-image"
)

var (
	testLabels = map[string]string{"a": "b"}

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
)

func init() {
	_ = dfv1.AddToScheme(scheme.Scheme)
	_ = appv1.AddToScheme(scheme.Scheme)
	_ = corev1.AddToScheme(scheme.Scheme)
}

func TestGetInstaller(t *testing.T) {

	fakeConfig := reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig)

	t.Run("get jetstream installer", func(t *testing.T) {
		installer, err := getInstaller(testJetStreamIsbSvc, nil, nil, fakeConfig, zaptest.NewLogger(t).Sugar(), nil)
		assert.NoError(t, err)
		assert.NotNil(t, installer)
		_, ok := installer.(*jetStreamInstaller)
		assert.True(t, ok)
	})

	t.Run("test error", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		testObj.Spec.JetStream = nil
		_, err := getInstaller(testObj, nil, nil, fakeConfig, zaptest.NewLogger(t).Sugar(), nil)
		assert.Error(t, err)
	})
}

func TestInstall(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	kubeClient := k8sfake.NewSimpleClientset()
	fakeConfig := reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig)
	ctx := context.TODO()

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
		testObj.Status.MarkChildrenResourceHealthy("RolloutFinished", "partitioned roll out complete: 3 new pods have been updated...")
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

	t.Run("test child resource not ready", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		testObj.Name = "fake-isb"
		err := Install(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.NoError(t, err)
		testObj.Status.MarkChildrenResourceUnHealthy("reason", "message")
		assert.False(t, testObj.Status.IsReady())
		assert.False(t, testObj.Status.IsHealthy())

	})
}

func TestUnInstall(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	kubeClient := k8sfake.NewSimpleClientset()
	fakeConfig := reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig)
	ctx := context.TODO()

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

	t.Run("test has pl connected", func(t *testing.T) {
		testPipeline := &dfv1.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pl",
				Namespace: testNamespace,
			},
			Spec: dfv1.PipelineSpec{
				InterStepBufferServiceName: testISBSName,
			},
		}
		err := cl.Create(ctx, testPipeline)
		assert.NoError(t, err)
		testObj := testJetStreamIsbSvc.DeepCopy()
		err = Uninstall(ctx, testObj, cl, kubeClient, fakeConfig, zaptest.NewLogger(t).Sugar(), record.NewFakeRecorder(64))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connected")
	})
}

func Test_referencedPipelines(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()

	t.Run("test no referenced pls", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		pls, err := referencedPipelines(ctx, cl, testObj)
		assert.NoError(t, err)
		assert.Equal(t, 0, pls)
	})

	t.Run("test having referenced pls - non default isbsvc", func(t *testing.T) {
		testPipeline := &dfv1.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pl",
				Namespace: testNamespace,
			},
			Spec: dfv1.PipelineSpec{
				InterStepBufferServiceName: testISBSName,
			},
		}
		err := cl.Create(ctx, testPipeline)
		assert.NoError(t, err)
		testObj := testJetStreamIsbSvc.DeepCopy()
		pls, err := referencedPipelines(ctx, cl, testObj)
		assert.NoError(t, err)
		assert.Equal(t, 1, pls)
	})

	t.Run("test having referenced pls - default isbsvc", func(t *testing.T) {
		testPipeline := &dfv1.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pl-1",
				Namespace: testNamespace,
			},
			Spec: dfv1.PipelineSpec{
				InterStepBufferServiceName: "",
			},
		}
		err := cl.Create(ctx, testPipeline)
		assert.NoError(t, err)
		testObj := testJetStreamIsbSvc.DeepCopy()
		testObj.Name = "default"
		pls, err := referencedPipelines(ctx, cl, testObj)
		assert.NoError(t, err)
		assert.Equal(t, 1, pls)
	})
}
