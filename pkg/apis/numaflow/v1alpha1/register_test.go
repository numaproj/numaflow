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
