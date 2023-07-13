package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidatePipelineCreate(t *testing.T) {
	pipeline := fakePipeline()
	v := NewPipelineValidator(fakeK8sClient, &fakePipelineClient, nil, pipeline)
	r := v.ValidateCreate(contextWithLogger(t))
	assert.True(t, r.Allowed)
}

func TestValidatePipelineUpdate(t *testing.T) {
	pipeline := fakePipeline()
	t.Run("test Pipeline interStepBufferServiceName change", func(t *testing.T) {
		newPipeline := pipeline.DeepCopy()
		newPipeline.Spec.InterStepBufferServiceName = "change-name"
		v := NewPipelineValidator(fakeK8sClient, &fakePipelineClient, pipeline, newPipeline)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
	})
}
