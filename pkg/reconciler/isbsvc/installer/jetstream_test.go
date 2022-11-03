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
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func init() {
	_ = dfv1.AddToScheme(scheme.Scheme)
	_ = appv1.AddToScheme(scheme.Scheme)
	_ = corev1.AddToScheme(scheme.Scheme)
}

func TestJetStreamBadInstallation(t *testing.T) {
	t.Run("bad installation", func(t *testing.T) {
		badIsbs := testJetStreamIsbSvc.DeepCopy()
		badIsbs.Spec.JetStream = nil
		installer := &jetStreamInstaller{
			client: fake.NewClientBuilder().Build(),
			isbs:   badIsbs,
			config: fakeConfig,
			labels: testLabels,
			logger: zaptest.NewLogger(t).Sugar(),
		}
		_, err := installer.Install(context.TODO())
		assert.Error(t, err)
	})
}

func TestJetStreamGenerateNames(t *testing.T) {
	n := generateJetStreamStatefulSetName(testJetStreamIsbSvc)
	assert.Equal(t, "isbsvc-"+testJetStreamIsbSvc.Name+"-js", n)
	n = generateJetStreamServerSecretName(testJetStreamIsbSvc)
	assert.Equal(t, "isbsvc-"+testJetStreamIsbSvc.Name+"-js-server", n)
	n = generateJetStreamClientAuthSecretName(testJetStreamIsbSvc)
	assert.Equal(t, "isbsvc-"+testJetStreamIsbSvc.Name+"-js-client-auth", n)
	n = generateJetStreamConfigMapName(testJetStreamIsbSvc)
	assert.Equal(t, "isbsvc-"+testJetStreamIsbSvc.Name+"-js-config", n)
	n = generateJetStreamPVCName(testJetStreamIsbSvc)
	assert.Equal(t, "isbsvc-"+testJetStreamIsbSvc.Name+"-js-vol", n)
	n = generateJetStreamServiceName(testJetStreamIsbSvc)
	assert.Equal(t, "isbsvc-"+testJetStreamIsbSvc.Name+"-js-svc", n)
}

func TestJetStreamCreateObjects(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()
	i := &jetStreamInstaller{
		client: cl,
		isbs:   testJetStreamIsbSvc,
		config: fakeConfig,
		labels: testLabels,
		logger: zaptest.NewLogger(t).Sugar(),
	}

	t.Run("test create sts", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createStatefulSet(ctx)
		assert.NoError(t, err)
		sts := &appv1.StatefulSet{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateJetStreamStatefulSetName(testObj)}, sts)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(sts.Spec.Template.Spec.Containers))
		assert.Contains(t, sts.Annotations, dfv1.KeyHash)
		assert.Equal(t, testJSImage, sts.Spec.Template.Spec.Containers[0].Image)
		assert.Equal(t, testJSReloaderImage, sts.Spec.Template.Spec.Containers[1].Image)
		assert.Equal(t, testJSMetricsImage, sts.Spec.Template.Spec.Containers[2].Image)
		assert.True(t, len(sts.Spec.Template.Spec.Volumes) > 1)
	})

	t.Run("test create svc", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createService(ctx)
		assert.NoError(t, err)
		svc := &corev1.Service{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateJetStreamServiceName(testObj)}, svc)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(svc.Spec.Ports))
		assert.Contains(t, svc.Annotations, dfv1.KeyHash)
	})

	t.Run("test create auth secrets", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createSecrets(ctx)
		assert.NoError(t, err)
		s := &corev1.Secret{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateJetStreamServerSecretName(testObj)}, s)
		assert.NoError(t, err)
		assert.Equal(t, 8, len(s.Data))
		assert.Contains(t, s.Data, dfv1.JetStreamServerSecretAuthKey)
		assert.Contains(t, s.Data, dfv1.JetStreamServerSecretEncryptionKey)
		assert.Contains(t, s.Data, dfv1.JetStreamServerCACertKey)
		assert.Contains(t, s.Data, dfv1.JetStreamServerCertKey)
		assert.Contains(t, s.Data, dfv1.JetStreamServerPrivateKeyKey)
		assert.Contains(t, s.Data, dfv1.JetStreamClusterCACertKey)
		assert.Contains(t, s.Data, dfv1.JetStreamClusterCertKey)
		assert.Contains(t, s.Data, dfv1.JetStreamClusterPrivateKeyKey)
		s = &corev1.Secret{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateJetStreamClientAuthSecretName(testObj)}, s)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(s.Data))
		assert.Contains(t, s.Data, dfv1.JetStreamClientAuthSecretUserKey)
		assert.Contains(t, s.Data, dfv1.JetStreamClientAuthSecretPasswordKey)
	})

	t.Run("test create configmap", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createConfigMap(ctx)
		assert.NoError(t, err)
		c := &corev1.ConfigMap{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateJetStreamConfigMapName(testObj)}, c)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(c.Data))
		assert.Contains(t, c.Annotations, dfv1.KeyHash)
	})
}

func Test_JetStreamInstall_Uninstall(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()
	i := &jetStreamInstaller{
		client: cl,
		isbs:   testJetStreamIsbSvc,
		config: fakeConfig,
		labels: testLabels,
		logger: zaptest.NewLogger(t).Sugar(),
	}
	t.Run("test install", func(t *testing.T) {
		c, err := i.Install(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.NotNil(t, c.JetStream)
		assert.NotEmpty(t, c.JetStream.URL)
		assert.NotNil(t, c.JetStream.Auth)
		assert.NotNil(t, c.JetStream.Auth.User)
		assert.NotNil(t, c.JetStream.Auth.Password)
		assert.True(t, testJetStreamIsbSvc.Status.IsReady())
		assert.False(t, c.JetStream.TLSEnabled)
		svc := &corev1.Service{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testJetStreamIsbSvc.Namespace, Name: generateJetStreamServiceName(testJetStreamIsbSvc)}, svc)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(svc.Spec.Ports))
		sts := &appv1.StatefulSet{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testJetStreamIsbSvc.Namespace, Name: generateJetStreamStatefulSetName(testJetStreamIsbSvc)}, sts)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(sts.Spec.Template.Spec.Containers))
	})

	t.Run("test uninstall", func(t *testing.T) {
		err := i.Uninstall(ctx)
		assert.NoError(t, err)
	})
}
