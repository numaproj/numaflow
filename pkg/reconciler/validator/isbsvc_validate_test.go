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
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

var (
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

	t.Run("test missing jetstream", func(t *testing.T) {
		isbs := testJetStreamIsbs.DeepCopy()
		isbs.Spec.JetStream = nil
		err := ValidateInterStepBufferService(isbs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), `"spec.jetstream" needs to be specified`)
	})

	t.Run("test missing jetstream version", func(t *testing.T) {
		isbs := testJetStreamIsbs.DeepCopy()
		isbs.Spec.JetStream.Version = ""
		err := ValidateInterStepBufferService(isbs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "is not defined")
	})

	t.Run("test nil ISB Service", func(t *testing.T) {
		err := ValidateInterStepBufferService(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nil ISB Service")
	})

	t.Run("test invalid name", func(t *testing.T) {
		isbs := testJetStreamIsbs.DeepCopy()
		isbs.Name = "UpperCaseName"
		err := ValidateInterStepBufferService(isbs)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid ISB Service name")
	})
}
