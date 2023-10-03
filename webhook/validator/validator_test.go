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

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func contextWithLogger(t *testing.T) context.Context {
	t.Helper()
	return logging.WithLogger(context.Background(), logging.NewLogger())
}

func TestGetValidator(t *testing.T) {
	t.Run("test get InterStepBufferService validator", func(t *testing.T) {
		bytes, err := json.Marshal(fakeISBSvc())
		assert.NoError(t, err)
		assert.NotNil(t, bytes)
		v, err := GetValidator(contextWithLogger(t), fakeK8sClient, &fakeNumaClient, metav1.GroupVersionKind{Group: "numaflow.numaproj.io", Version: "v1alpha1", Kind: "InterStepBufferService"}, nil, bytes)
		assert.NoError(t, err)
		assert.NotNil(t, v)
	})

	t.Run("test get Pipeline validator", func(t *testing.T) {
		bytes, err := json.Marshal(fakePipeline())
		assert.NoError(t, err)
		assert.NotNil(t, bytes)
		v, err := GetValidator(contextWithLogger(t), fakeK8sClient, &fakeNumaClient, metav1.GroupVersionKind{Group: "numaflow.numaproj.io", Version: "v1alpha1", Kind: "Pipeline"}, nil, bytes)
		assert.NoError(t, err)
		assert.NotNil(t, v)
	})
}
