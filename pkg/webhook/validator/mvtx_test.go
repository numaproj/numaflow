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
	"k8s.io/utils/ptr"
)

func TestValidateMonoVertexCreate(t *testing.T) {
	mvtx := fakeMonoVertex()

	t.Run("test create ok", func(t *testing.T) {
		v := NewMonoVertexValidator(nil, mvtx)
		r := v.ValidateCreate(contextWithLogger(t))
		assert.True(t, r.Allowed)
	})

	t.Run("test create with no source def", func(t *testing.T) {
		newMvtx := mvtx.DeepCopy()
		newMvtx.Spec.Source = nil
		v := NewMonoVertexValidator(nil, newMvtx)
		r := v.ValidateCreate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "source is not defined")
	})

	t.Run("test create with min above max", func(t *testing.T) {
		newMvtx := mvtx.DeepCopy()
		newMvtx.Spec.Scale.Min = ptr.To[int32](10)
		newMvtx.Spec.Scale.Max = ptr.To[int32](1)
		v := NewMonoVertexValidator(nil, newMvtx)
		r := v.ValidateCreate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "invalid scale")
	})
}

func TestValidateMonoVertexUpdate(t *testing.T) {
	mvtx := fakeMonoVertex()

	t.Run("test update ok", func(t *testing.T) {
		newMvtx := mvtx.DeepCopy()
		newMvtx.Spec.Source.UDSource.Container.Image = "newimage"
		v := NewMonoVertexValidator(mvtx, newMvtx)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.True(t, r.Allowed)
	})

	t.Run("test update with no source def", func(t *testing.T) {
		newMvtx := mvtx.DeepCopy()
		newMvtx.Spec.Source = nil
		v := NewMonoVertexValidator(mvtx, newMvtx)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "source is not defined")
	})

	t.Run("test update with min above max", func(t *testing.T) {
		newMvtx := mvtx.DeepCopy()
		newMvtx.Spec.Scale.Min = ptr.To[int32](10)
		newMvtx.Spec.Scale.Max = ptr.To[int32](1)
		v := NewMonoVertexValidator(mvtx, newMvtx)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
		assert.Contains(t, r.Result.Message, "invalid scale")
	})
}
