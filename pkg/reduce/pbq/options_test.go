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

package pbq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	testOpts := []PBQOption{
		WithReadBatchSize(100),
		WithChannelBufferSize(10),
		WithReadTimeout(2 * time.Second),
	}

	queueOption := &options{
		channelBufferSize: 5,
		readTimeout:       1,
		readBatchSize:     5,
	}

	for _, opt := range testOpts {
		err := opt(queueOption)
		assert.NoError(t, err)
	}

	assert.Equal(t, int64(100), queueOption.readBatchSize)
	assert.Equal(t, int64(10), queueOption.channelBufferSize)
	assert.Equal(t, 2*time.Second, queueOption.readTimeout)
}
