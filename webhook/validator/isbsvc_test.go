package validator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateISBServiceCreate(t *testing.T) {
	isbsvc := fakeISBSvc()
	v := NewISBServiceValidator(fakeK8sClient, &fakeISBSvcClient, nil, isbsvc)
	r := v.ValidateCreate(contextWithLogger(t))
	assert.True(t, r.Allowed)
}

func TestValidateISBServiceUpdate(t *testing.T) {
	isbsvc := fakeISBSvc()
	t.Run("test ISBSvc spec change", func(t *testing.T) {
		JetStreamISBSvc := fakeJetStreamISBSvc()
		newISBSvc := JetStreamISBSvc.DeepCopy()
		v := NewISBServiceValidator(fakeK8sClient, &fakeISBSvcClient, isbsvc, newISBSvc)
		r := v.ValidateUpdate(contextWithLogger(t))
		assert.False(t, r.Allowed)
	})
}
