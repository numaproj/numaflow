package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func Test_ISBSvcSetPhase(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.SetPhase(ISBSvcPhasePending, "message")
	assert.Equal(t, "message", s.Message)
	assert.Equal(t, ISBSvcPhasePending, s.Phase)
}

func Test_ISBSvcSetType(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.SetType(ISBSvcTypeJetStream)
	assert.Equal(t, ISBSvcTypeJetStream, s.Type)
}

func Test_ISBSvcInitConditions(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.InitConditions()
	assert.Equal(t, 2, len(s.Conditions))
	for _, c := range s.Conditions {
		assert.Equal(t, metav1.ConditionUnknown, c.Status)
	}
	assert.Equal(t, ISBSvcPhasePending, s.Phase)
}

func Test_ISBSvcMarkStatus(t *testing.T) {
	s := InterStepBufferServiceStatus{}
	s.InitConditions()
	s.MarkNotConfigured("reason", "message")
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionConfigured) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
		}
	}
	s.MarkConfigured()
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionConfigured) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
		}
	}
	s.MarkDeployFailed("reason", "message")
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionDeployed) {
			assert.Equal(t, metav1.ConditionFalse, c.Status)
			assert.Equal(t, "reason", c.Reason)
			assert.Equal(t, "message", c.Message)
		}
	}
	s.MarkDeployed()
	for _, c := range s.Conditions {
		if c.Type == string(ISBSvcConditionDeployed) {
			assert.Equal(t, metav1.ConditionTrue, c.Status)
		}
	}
	assert.True(t, s.IsReady())
}
