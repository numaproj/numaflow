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

func TestValidateISBServiceCreate(t *testing.T) {
	isbsvc := fakeRedisISBSvc()
	v := NewISBServiceValidator(nil, isbsvc)
	r := v.ValidateCreate(contextWithLogger(t))
	assert.True(t, r.Allowed)
}

func TestValidateISBServiceUpdate(t *testing.T) {
	testCases := []struct {
		name string
		old  *dfv1.InterStepBufferService
		new  *dfv1.InterStepBufferService
		want bool
	}{
		{name: "invalid new ISBSvc spec", old: fakeRedisISBSvc(), new: nil, want: false},
		{name: "changing ISB Service type is not allowed - redis to jetstream", old: fakeRedisISBSvc(), new: fakeJetStreamISBSvc(), want: false},
		{name: "changing ISB Service type is not allowed - jetstream to redis", old: fakeJetStreamISBSvc(), new: fakeRedisISBSvc(), want: false},
		{name: "valid new ISBSvc spec", old: fakeRedisISBSvc(), new: fakeRedisISBSvc(), want: true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			v := NewISBServiceValidator(tc.old, tc.new)
			r := v.ValidateUpdate(contextWithLogger(t))
			assert.Equal(t, tc.want, r.Allowed)
		})
	}
}
