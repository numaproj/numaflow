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

package scaling

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func Test_BasicOperations(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	s := NewScaler(cl)
	assert.NotNil(t, s)
	s.StartWatching("key1")
	assert.True(t, s.Contains("key1"))
	assert.Equal(t, 1, s.Length())
	s.StopWatching("key1")
	assert.False(t, s.Contains("key1"))
}

func Test_desiredReplicas(t *testing.T) {
	cl := fake.NewClientBuilder().Build()
	s := NewScaler(cl)
	one := uint32(1)
	src := &dfv1.Vertex{
		Spec: dfv1.VertexSpec{
			Replicas: pointer.Int32(2),
			AbstractVertex: dfv1.AbstractVertex{
				Source: &dfv1.Source{
					Kafka: &dfv1.KafkaSource{},
				},
				Scale: dfv1.Scale{
					TargetProcessingSeconds: &one,
				},
			},
		},
		Status: dfv1.VertexStatus{
			Replicas: uint32(2),
		},
	}
	assert.Equal(t, int32(2), s.desiredReplicas(context.TODO(), src, []float64{0}, []int64{0}, []int64{10000}, []int64{5000}))
	assert.Equal(t, int32(8), s.desiredReplicas(context.TODO(), src, []float64{2500}, []int64{10010}, []int64{30000}, []int64{20000}))
	assert.Equal(t, int32(8), s.desiredReplicas(context.TODO(), src, []float64{2500}, []int64{9950}, []int64{30000}, []int64{20000}))
	assert.Equal(t, int32(7), s.desiredReplicas(context.TODO(), src, []float64{2500}, []int64{8751}, []int64{30000}, []int64{20000}))
	assert.Equal(t, int32(7), s.desiredReplicas(context.TODO(), src, []float64{2500}, []int64{8749}, []int64{30000}, []int64{20000}))
	assert.Equal(t, int32(2), s.desiredReplicas(context.TODO(), src, []float64{0}, []int64{9950}, []int64{30000}, []int64{20000}))
	assert.Equal(t, int32(1), s.desiredReplicas(context.TODO(), src, []float64{2500}, []int64{2}, []int64{30000}, []int64{20000}))
	assert.Equal(t, int32(2), s.desiredReplicas(context.TODO(), src, []float64{2500}, []int64{0}, []int64{30000}, []int64{20000}))

	udf := &dfv1.Vertex{
		Spec: dfv1.VertexSpec{
			Replicas: pointer.Int32(2),
			AbstractVertex: dfv1.AbstractVertex{
				UDF: &dfv1.UDF{},
			},
		},
		Status: dfv1.VertexStatus{
			Replicas: uint32(2),
		},
	}
	assert.Equal(t, int32(2), s.desiredReplicas(context.TODO(), udf, []float64{0}, []int64{0}, []int64{10000}, []int64{5000}))
	assert.Equal(t, int32(1), s.desiredReplicas(context.TODO(), udf, []float64{250}, []int64{10000}, []int64{20000}, []int64{5000}))
	assert.Equal(t, int32(1), s.desiredReplicas(context.TODO(), udf, []float64{250}, []int64{10000}, []int64{20000}, []int64{6000}))
	assert.Equal(t, int32(2), s.desiredReplicas(context.TODO(), udf, []float64{250}, []int64{10000}, []int64{20000}, []int64{7500}))
	assert.Equal(t, int32(2), s.desiredReplicas(context.TODO(), udf, []float64{250}, []int64{10000}, []int64{20000}, []int64{7900}))
	assert.Equal(t, int32(2), s.desiredReplicas(context.TODO(), udf, []float64{250}, []int64{10000}, []int64{20000}, []int64{10000}))
	assert.Equal(t, int32(3), s.desiredReplicas(context.TODO(), udf, []float64{250}, []int64{10000}, []int64{20000}, []int64{12500}))
	assert.Equal(t, int32(3), s.desiredReplicas(context.TODO(), udf, []float64{250}, []int64{10000}, []int64{20000}, []int64{12550}))
}
