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
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
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
			client:   fake.NewClientBuilder().Build(),
			isbSvc:   badIsbs,
			config:   reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			labels:   testLabels,
			logger:   zaptest.NewLogger(t).Sugar(),
			recorder: record.NewFakeRecorder(64),
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
		client:     cl,
		kubeClient: k8sfake.NewSimpleClientset(),
		isbSvc:     testJetStreamIsbSvc,
		config:     reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
		labels:     testLabels,
		logger:     zaptest.NewLogger(t).Sugar(),
		recorder:   record.NewFakeRecorder(64),
	}

	t.Run("test create sts", func(t *testing.T) {
		testObj := testJetStreamIsbSvc.DeepCopy()
		i.isbSvc = testObj
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
		i.isbSvc = testObj
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
		i.isbSvc = testObj
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
		i.isbSvc = testObj
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
		client:     cl,
		kubeClient: k8sfake.NewSimpleClientset(),
		isbSvc:     testJetStreamIsbSvc,
		config:     reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
		labels:     testLabels,
		logger:     zaptest.NewLogger(t).Sugar(),
		recorder:   record.NewFakeRecorder(64),
	}
	t.Run("test install", func(t *testing.T) {
		c, err := i.Install(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.NotNil(t, c.JetStream)
		assert.NotEmpty(t, c.JetStream.URL)
		assert.NotNil(t, c.JetStream.Auth)
		assert.NotNil(t, c.JetStream.Auth.Basic)
		assert.NotNil(t, c.JetStream.Auth.Basic.User)
		assert.NotNil(t, c.JetStream.Auth.Basic.Password)
		assert.True(t, testJetStreamIsbSvc.Status.IsReady())
		assert.True(t, testJetStreamIsbSvc.Status.IsHealthy())
		assert.False(t, c.JetStream.TLSEnabled)
		events := getEvents(i.recorder)
		assert.Contains(t, events, "Normal JetStreamConfigMap Created jetstream configmap successfully", "Normal JetStreamServiceSuccess Created jetstream service successfully", "Normal JetStreamStatefulSetSuccess Created jetstream stateful successfully")
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

func TestCheckChildrenResourceStatusQuorum(t *testing.T) {
	ctx := context.TODO()
	stsName := generateJetStreamStatefulSetName(testJetStreamIsbSvc)
	replicas := int32(3)
	baseSts := &appv1.StatefulSet{
		ObjectMeta: testJetStreamIsbSvc.ObjectMeta,
		Spec: appv1.StatefulSetSpec{
			Replicas: &replicas,
		},
		Status: appv1.StatefulSetStatus{
			ObservedGeneration: 1,
			CurrentRevision:    "rev-1",
			UpdateRevision:     "rev-1",
			CurrentReplicas:    3,
			Replicas:           3,
		},
	}
	baseSts.Name = stsName

	childrenHealthy := func(isbSvc *dfv1.InterStepBufferService) bool {
		c := isbSvc.Status.GetCondition(dfv1.ISBSvcConditionChildrenResourcesHealthy)
		return c != nil && c.Status == "True"
	}

	t.Run("healthy at quorum (2/3 ready)", func(t *testing.T) {
		sts := baseSts.DeepCopy()
		sts.Status.ReadyReplicas = 2
		isbSvc := testJetStreamIsbSvc.DeepCopy()
		cl := fake.NewClientBuilder().WithObjects(sts).Build()
		i := &jetStreamInstaller{
			client:     cl,
			kubeClient: k8sfake.NewSimpleClientset(),
			isbSvc:     isbSvc,
			config:     reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			labels:     testLabels,
			logger:     zaptest.NewLogger(t).Sugar(),
			recorder:   record.NewFakeRecorder(64),
		}
		err := i.CheckChildrenResourceStatus(ctx)
		assert.NoError(t, err)
		assert.True(t, childrenHealthy(isbSvc), "2/3 ready should be healthy at quorum=2")
	})

	t.Run("unhealthy below quorum (1/3 ready)", func(t *testing.T) {
		sts := baseSts.DeepCopy()
		sts.Status.ReadyReplicas = 1
		isbSvc := testJetStreamIsbSvc.DeepCopy()
		cl := fake.NewClientBuilder().WithObjects(sts).Build()
		i := &jetStreamInstaller{
			client:     cl,
			kubeClient: k8sfake.NewSimpleClientset(),
			isbSvc:     isbSvc,
			config:     reconciler.FakeGlobalConfig(t, fakeGlobalISBSvcConfig),
			labels:     testLabels,
			logger:     zaptest.NewLogger(t).Sugar(),
			recorder:   record.NewFakeRecorder(64),
		}
		err := i.CheckChildrenResourceStatus(ctx)
		assert.NoError(t, err)
		assert.False(t, childrenHealthy(isbSvc), "1/3 ready should be unhealthy below quorum=2")
	})
}

func getEvents(recorder record.EventRecorder) []string {
	c := recorder.(*record.FakeRecorder).Events
	close(c)
	events := make([]string, len(c))
	for msg := range c {
		events = append(events, msg)
	}
	return events
}
