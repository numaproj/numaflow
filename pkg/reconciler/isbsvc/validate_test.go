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
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	testRedisIsbs = &dfv1.InterStepBufferService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "test-ns",
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

	testJetStreamIsbs = &dfv1.InterStepBufferService{
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
)

func TestValidateInterStepBuffer(t *testing.T) {
	t.Run("test good redis isb", func(t *testing.T) {
		err := ValidateInterStepBufferService(testRedisIsbs)
		assert.NoError(t, err)
	})

	t.Run("test both redis and jetstream configured", func(t *testing.T) {
		isbs := testJetStreamIsbs.DeepCopy()
		isbs.Spec.Redis = testRedisIsbs.DeepCopy().Spec.Redis
		err := ValidateInterStepBufferService(isbs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `spec.redis" and "spec.jetstream" can not be defined together`)
	})

	t.Run("test missing spec.redis", func(t *testing.T) {
		isbs := testRedisIsbs.DeepCopy()
		isbs.Spec.Redis = nil
		err := ValidateInterStepBufferService(isbs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `either "spec.redis" or "spec.jetstream" needs to be specified`)
	})

	t.Run("test missing version", func(t *testing.T) {
		isbs := testRedisIsbs.DeepCopy()
		isbs.Spec.Redis.Native.Version = ""
		err := ValidateInterStepBufferService(isbs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"spec.redis.native.version" is not defined`)
	})

	t.Run("test both native and external configured", func(t *testing.T) {
		isbs := testRedisIsbs.DeepCopy()
		isbs.Spec.Redis.External = &dfv1.RedisConfig{}
		err := ValidateInterStepBufferService(isbs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "can not be defined together")
	})

	t.Run("test neither native or external configured", func(t *testing.T) {
		isbs := testRedisIsbs.DeepCopy()
		isbs.Spec.Redis.Native = nil
		err := ValidateInterStepBufferService(isbs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be defined")
	})

	t.Run("test missing jetstream version", func(t *testing.T) {
		isbs := testJetStreamIsbs.DeepCopy()
		isbs.Spec.JetStream.Version = ""
		err := ValidateInterStepBufferService(isbs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not defined")
	})
}
