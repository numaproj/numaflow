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

package nats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultOptions(t *testing.T) {
	opts := defaultOptions()
	assert.NotNil(t, opts)
	assert.Equal(t, 3, opts.clientPoolSize, "default client pool size should be 3")
}

func TestWithClientPoolSize(t *testing.T) {
	opts := defaultOptions()
	assert.Equal(t, 3, opts.clientPoolSize, "default client pool size should be 3")

	option := WithClientPoolSize(10)
	option(opts)

	assert.Equal(t, 10, opts.clientPoolSize, "client pool size should be set to 10")
}

func TestCombinedOptions(t *testing.T) {
	opts := defaultOptions()
	assert.Equal(t, 3, opts.clientPoolSize, "default client pool size should be 3")

	option1 := WithClientPoolSize(5)
	option1(opts)

	assert.Equal(t, 5, opts.clientPoolSize, "client pool size should be set to 5")
}
