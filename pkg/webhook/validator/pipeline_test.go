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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func TestValidatePipelineCreate(t *testing.T) {
	pipeline := fakePipeline()
	fk := MockInterStepBufferServices{}

	t.Run("test create ok", func(t *testing.T) {
		v := NewPipelineValidator(&fk, nil, pipeline)
		r := v.ValidateCreate(contextWithLogger(t))
		assert.True(t, r.Allowed)
	})

	t.Run("test create with pipeline and isbsvc instance annotation mismatch", func(t *testing.T) {
		newPipeline := pipeline.DeepCopy()
		newPipeline.Annotations[dfv1.KeyInstance] = "abc"
		v := NewPipelineValidator(&fk, pipeline, newPipeline)
		r := v.ValidateCreate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "does not have the same annotation")
	})

}

func TestValidatePipelineUpdate(t *testing.T) {
	pipeline := fakePipeline()
	fk := MockInterStepBufferServices{}
	t.Run("test old pipeline spec is nil", func(t *testing.T) {
		v := NewPipelineValidator(&fk, nil, pipeline)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "old pipeline spec is nil")
	})

	t.Run("test invalid new pipeline spec", func(t *testing.T) {
		v := NewPipelineValidator(&fk, pipeline, nil)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "new pipeline spec is invalid")
	})

	t.Run("test pipeline interStepBufferServiceName change", func(t *testing.T) {
		newPipeline := pipeline.DeepCopy()
		newPipeline.Spec.InterStepBufferServiceName = "change-name"
		v := NewPipelineValidator(&fk, pipeline, newPipeline)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "different ISB service name")
	})

	t.Run("test pipeline instance annotation change", func(t *testing.T) {
		newPipeline := pipeline.DeepCopy()
		newPipeline.Annotations[dfv1.KeyInstance] = "change-name"
		v := NewPipelineValidator(&fk, pipeline, newPipeline)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "cannot update pipeline with different annotation")
	})

	t.Run("test should not change the type of a vertex", func(t *testing.T) {
		newPipeline := pipeline.DeepCopy()
		// in our test fake pipeline, the 3nd vertex is a reduce vertex
		// change it to a map vertex, this ensures that the new pipeline is still valid but the update is not allowed
		newPipeline.Spec.Vertices[2].UDF = &dfv1.UDF{
			Container: &dfv1.Container{
				Image: "my-image",
			},
		}
		v := NewPipelineValidator(&fk, pipeline, newPipeline)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "vertex type is immutable")
	})

	t.Run("test should not change the partition count of a reduce vertex", func(t *testing.T) {
		var oldPartitionCount, newPartitionCount int32 = 2, 3
		newPipeline := pipeline.DeepCopy()
		// in our test fake pipeline, the 3rd vertex is a reduce vertex
		pipeline.Spec.Vertices[2].Partitions = &oldPartitionCount
		newPipeline.Spec.Vertices[2].Partitions = &newPartitionCount
		v := NewPipelineValidator(&fk, pipeline, newPipeline)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "partition count is immutable for a reduce vertex")
	})

	t.Run("test should not change the persistent storage of a reduce vertex", func(t *testing.T) {
		newPipeline := pipeline.DeepCopy()
		newPipeline.Spec.Vertices[2].UDF.GroupBy.Storage = &dfv1.PBQStorage{
			PersistentVolumeClaim: &dfv1.PersistenceStrategy{},
		}
		v := NewPipelineValidator(&fk, pipeline, newPipeline)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "storage is immutable for a reduce vertex")
	})
}
