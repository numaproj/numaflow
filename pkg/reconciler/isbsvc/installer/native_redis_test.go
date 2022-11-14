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

func TestNativeRedisBadInstallation(t *testing.T) {
	t.Run("bad installation", func(t *testing.T) {
		badIsbs := testNativeRedisIsbSvc.DeepCopy()
		badIsbs.Spec.Redis = nil
		installer := &redisInstaller{
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

func TestNativeRedisGenerateNames(t *testing.T) {
	n := generateRedisStatefulSetName(testNativeRedisIsbSvc)
	assert.Equal(t, "isbsvc-"+testNativeRedisIsbSvc.Name+"-redis", n)
	n = generateRedisCredentialSecretName(testNativeRedisIsbSvc)
	assert.Equal(t, "isbsvc-"+testNativeRedisIsbSvc.Name+"-redis-auth", n)
	n = generateHealthConfigMapName(testNativeRedisIsbSvc)
	assert.Equal(t, "isbsvc-"+testNativeRedisIsbSvc.Name+"-redis-health", n)
	n = generateRedisPVCName(testNativeRedisIsbSvc)
	assert.Equal(t, "isbsvc-"+testNativeRedisIsbSvc.Name+"-redis-vol", n)
	n = generateRedisConfigMapName(testNativeRedisIsbSvc)
	assert.Equal(t, "isbsvc-"+testNativeRedisIsbSvc.Name+"-redis-config", n)
	n = generateRedisHeadlessServiceName(testNativeRedisIsbSvc)
	assert.Equal(t, "isbsvc-"+testNativeRedisIsbSvc.Name+"-redis-svc-headless", n)
	n = generateRedisServiceName(testNativeRedisIsbSvc)
	assert.Equal(t, "isbsvc-"+testNativeRedisIsbSvc.Name+"-redis-svc", n)
	n = generateScriptsConfigMapName(testNativeRedisIsbSvc)
	assert.Equal(t, "isbsvc-"+testNativeRedisIsbSvc.Name+"-redis-scripts", n)
}

func TestNativeRedisCreateObjects(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()
	i := &redisInstaller{
		client: cl,
		isbs:   testNativeRedisIsbSvc,
		config: fakeConfig,
		labels: testLabels,
		logger: zaptest.NewLogger(t).Sugar(),
	}

	t.Run("test create sts", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createStatefulSet(ctx)
		assert.NoError(t, err)
		sts := &appv1.StatefulSet{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateRedisStatefulSetName(testObj)}, sts)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(sts.Spec.Template.Spec.Containers))
		assert.Contains(t, sts.Annotations, dfv1.KeyHash)
		assert.Equal(t, testImage, sts.Spec.Template.Spec.Containers[0].Image)
		assert.Equal(t, testSImage, sts.Spec.Template.Spec.Containers[1].Image)
		assert.Equal(t, testRedisExporterImage, sts.Spec.Template.Spec.Containers[2].Image)
		assert.True(t, len(sts.Spec.Template.Spec.Volumes) > 1)
	})

	t.Run("test create redis svc", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createRedisService(ctx)
		assert.NoError(t, err)
		svc := &corev1.Service{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateRedisServiceName(testObj)}, svc)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(svc.Spec.Ports))
		assert.Contains(t, svc.Annotations, dfv1.KeyHash)
	})

	t.Run("test create redis headless svc", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createRedisHeadlessService(ctx)
		assert.NoError(t, err)
		svc := &corev1.Service{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateRedisHeadlessServiceName(testObj)}, svc)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(svc.Spec.Ports))
		assert.Contains(t, svc.Annotations, dfv1.KeyHash)
	})

	t.Run("test create redis auth secret", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createAuthCredentialSecret(ctx)
		assert.NoError(t, err)
		s := &corev1.Secret{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateRedisCredentialSecretName(testObj)}, s)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(s.Data))
		assert.Contains(t, s.Data, dfv1.RedisAuthSecretKey)
	})

	t.Run("test create redis config", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createConfConfigMap(ctx)
		assert.NoError(t, err)
		c := &corev1.ConfigMap{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateRedisConfigMapName(testObj)}, c)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(c.Data))
		assert.Contains(t, c.Annotations, dfv1.KeyHash)
	})

	t.Run("test create redis scripts config", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createScriptsConfigMap(ctx)
		assert.NoError(t, err)
		c := &corev1.ConfigMap{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateScriptsConfigMapName(testObj)}, c)
		assert.NoError(t, err)
		assert.Equal(t, 4, len(c.Data))
		assert.Contains(t, c.Annotations, dfv1.KeyHash)
	})

	t.Run("test create redis health config", func(t *testing.T) {
		testObj := testNativeRedisIsbSvc.DeepCopy()
		i.isbs = testObj
		err := i.createHealthConfigMap(ctx)
		assert.NoError(t, err)
		c := &corev1.ConfigMap{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testObj.Namespace, Name: generateHealthConfigMapName(testObj)}, c)
		assert.NoError(t, err)
		assert.Equal(t, 8, len(c.Data))
		assert.Contains(t, c.Annotations, dfv1.KeyHash)
	})
}

func Test_NativeRedisInstall_Uninstall(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	ctx := context.TODO()
	i := &redisInstaller{
		client: cl,
		isbs:   testNativeRedisIsbSvc,
		config: fakeConfig,
		labels: testLabels,
		logger: zaptest.NewLogger(t).Sugar(),
	}
	t.Run("test install", func(t *testing.T) {
		c, err := i.Install(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.NotNil(t, c.Redis)
		assert.NotEmpty(t, c.Redis.SentinelURL)
		assert.NotEmpty(t, c.Redis.MasterName)
		assert.NotEmpty(t, c.Redis.User)
		assert.NotNil(t, c.Redis.Password)
		assert.NotNil(t, c.Redis.SentinelPassword)
		assert.True(t, testNativeRedisIsbSvc.Status.IsReady())
		svc := &corev1.Service{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testNativeRedisIsbSvc.Namespace, Name: generateRedisHeadlessServiceName(testNativeRedisIsbSvc)}, svc)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(svc.Spec.Ports))
		sts := &appv1.StatefulSet{}
		err = cl.Get(ctx, types.NamespacedName{Namespace: testNativeRedisIsbSvc.Namespace, Name: generateRedisStatefulSetName(testNativeRedisIsbSvc)}, sts)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(sts.Spec.Template.Spec.Containers))
	})

	t.Run("test uninstall", func(t *testing.T) {
		err := i.Uninstall(ctx)
		assert.NoError(t, err)
	})
}
