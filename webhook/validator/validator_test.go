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

package validator

import (
	"context"
	"encoding/json"
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	fake "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1/fake"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeClient "k8s.io/client-go/kubernetes/fake"
)

const (
	testNamespace = "test-ns"
)

var (
	fakeK8sClient      = fakeClient.NewSimpleClientset()
	fakeISBSvcClient   = fake.FakeInterStepBufferServices{}
	fakePipelineClient = fake.FakePipelines{}
)

func contextWithLogger(t *testing.T) context.Context {
	t.Helper()
	return logging.WithLogger(context.Background(), logging.NewLogger())
}

func fakeISBSvc() *dfv1.InterStepBufferService {
	return &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: testNamespace,
			Name:      dfv1.DefaultISBSvcName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{
			Redis: &dfv1.RedisBufferService{
				Native: &dfv1.NativeRedis{
					Version: "6.2.6",
				},
			},
		},
	}
}

func fakeJetStreamISBSvc() *dfv1.InterStepBufferService {
	return &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test-ns",
			Name:      dfv1.DefaultISBSvcName,
		},
		Spec: dfv1.InterStepBufferServiceSpec{
			JetStream: &dfv1.JetStreamBufferService{
				Version: "1.1.1",
			},
		},
	}
}

func fakePipeline() *dfv1.Pipeline {
	return &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pl",
			Namespace: "test-ns",
		},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{
					Name: "input",
					Source: &dfv1.Source{
						UDTransformer: &dfv1.UDTransformer{
							Builtin: &dfv1.Transformer{Name: "filter"},
						}},
				},
				{
					Name: "p1",
					UDF: &dfv1.UDF{
						Builtin: &dfv1.Function{Name: "cat"},
					},
				},
				{
					Name: "output",
					Sink: &dfv1.Sink{},
				},
			},
			Edges: []dfv1.Edge{
				{From: "input", To: "p1"},
				{From: "p1", To: "output"},
			},
		},
	}
}

func TestGetValidator(t *testing.T) {
	t.Run("test get InterStepBufferService validator", func(t *testing.T) {
		bytes, err := json.Marshal(fakeISBSvc())
		assert.NoError(t, err)
		assert.NotNil(t, bytes)
		v, err := GetValidator(contextWithLogger(t), fakeK8sClient, &fakeISBSvcClient, &fakePipelineClient, metav1.GroupVersionKind{Group: "numaflow.numaproj.io", Version: "v1alpha1", Kind: "InterStepBufferService"}, nil, bytes)
		assert.NoError(t, err)
		assert.NotNil(t, v)
	})

	t.Run("test get Pipeline validator", func(t *testing.T) {
		bytes, err := json.Marshal(fakePipeline())
		assert.NoError(t, err)
		assert.NotNil(t, bytes)
		v, err := GetValidator(contextWithLogger(t), fakeK8sClient, &fakeISBSvcClient, &fakePipelineClient, metav1.GroupVersionKind{Group: "numaflow.numaproj.io", Version: "v1alpha1", Kind: "Pipeline"}, nil, bytes)
		assert.NoError(t, err)
		assert.NotNil(t, v)
	})
}
