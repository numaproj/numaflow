package v1alpha1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/client-go/kubernetes/scheme"
)

func TestRegisterResource(t *testing.T) {
	a := Resource("abc")
	assert.Equal(t, "numaflow.numaproj.io", a.Group)
	assert.Equal(t, "abc", a.Resource)
}

func TestAddKnownTypes(t *testing.T) {
	err := addKnownTypes(scheme.Scheme)
	assert.NoError(t, err)
}
