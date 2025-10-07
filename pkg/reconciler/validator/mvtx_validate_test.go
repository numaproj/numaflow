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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
)

var (
	testMvtx = &dfv1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pl",
			Namespace: "test-ns",
		},
		Spec: dfv1.MonoVertexSpec{
			InitContainers: []corev1.Container{
				{
					Name:  "init-container",
					Image: "my-image:latest",
				},
			},
			Sidecars: []corev1.Container{
				{
					Name:  "sidecar-container",
					Image: "my-image:latest",
				},
			},
			Source: &dfv1.Source{
				UDTransformer: &dfv1.UDTransformer{
					Container: &dfv1.Container{Image: "my-image"},
				},
				UDSource: &dfv1.UDSource{
					Container: &dfv1.Container{
						Image: "my-image:latest",
					},
				},
			},
			Sink: &dfv1.Sink{
				AbstractSink: dfv1.AbstractSink{
					UDSink: &dfv1.UDSink{
						Container: &dfv1.Container{
							Image: "my-image:latest",
						},
					},
				},
				Fallback: &dfv1.AbstractSink{
					UDSink: &dfv1.UDSink{
						Container: &dfv1.Container{
							Image: "my-fb-image:latest",
						},
					},
				},
			},
			UDF: &dfv1.UDF{
				Container: &dfv1.Container{
					Image: "my-udf-image:latest",
				},
			},
		},
	}
)

func TestValidateMonoVertex(t *testing.T) {
	t.Run("test good mvtx", func(t *testing.T) {
		err := ValidateMonoVertex(testMvtx)
		assert.NoError(t, err)
	})

	t.Run("test nil", func(t *testing.T) {
		err := ValidateMonoVertex(nil)
		assert.Error(t, err)
	})

	t.Run("test invalid name", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Name = "test-pl-iNvalid+name"
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid mvtx name")
	})

	t.Run("test no source", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Source = nil
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "source is not defined")
	})

	t.Run("test invalid source", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Source.Kafka = &dfv1.KafkaSource{}
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid user-defined source spec")
	})

	t.Run("test no sink", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Sink = nil
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "sink is not defined")
	})

	t.Run("test invalid sink", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Sink.Fallback = nil
		testObj.Spec.Sink.RetryStrategy = dfv1.RetryStrategy{
			OnFailure: ptr.To(dfv1.OnFailureFallback),
		}
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "given OnFailure strategy is fallback but fallback sink is not provided")
	})

	t.Run("test invalid init container name", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.InitContainers[0].Name = dfv1.CtrInitSideInputs
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid init container name")
	})

	t.Run("test invalid sidecar container name", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.Sidecars[0].Name = dfv1.CtrInitSideInputs
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid sidecar container name")
	})

	t.Run("test invalid maxUnavailable", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		testObj.Spec.UpdateStrategy = dfv1.UpdateStrategy{
			RollingUpdate: &dfv1.RollingUpdateStrategy{
				MaxUnavailable: &intstr.IntOrString{
					Type:   intstr.String,
					StrVal: "invalid",
				},
			},
		}
		err := ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid maxUnavailable")
	})

	t.Run("test udf spec validation", func(t *testing.T) {
		testObj := testMvtx.DeepCopy()
		err := ValidateMonoVertex(testObj)
		assert.NoError(t, err)

		// GroupBy is not allowed in MonoVertex UDF
		testObj.Spec.UDF.GroupBy = &dfv1.GroupBy{
			Storage: &dfv1.PBQStorage{
				PersistentVolumeClaim: &dfv1.PersistenceStrategy{},
			},
		}
		err = ValidateMonoVertex(testObj)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid udf: groupBy/reduce is not supported in monovertex")

		// UDF is optional so nil should be allowed
		testObj.Spec.UDF = nil
		err = ValidateMonoVertex(testObj)
		assert.NoError(t, err)
	})
}
