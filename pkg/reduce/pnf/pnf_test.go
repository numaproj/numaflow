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
package pnf

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

const (
	testPipelineName = "testPipeline"
)

type forwardTest struct {
	count   int
	buffers []string
}

func (f *forwardTest) WhereTo(_ []string, _ []string, s string) ([]forwarder.VertexBuffer, error) {
	var steps []forwarder.VertexBuffer
	for _, buffer := range f.buffers {
		steps = append(steps, forwarder.VertexBuffer{
			ToVertexName:         buffer,
			ToVertexPartitionIdx: int32(f.count % 2),
		})
	}
	f.count++
	return steps, nil
}

// TestWriteToBuffer tests two BufferFullWritingStrategies: 1. discarding the latest message and 2. retrying writing until context is cancelled.
func TestWriteToBuffer(t *testing.T) {
	testStartTime := time.Unix(1636470000, 0).UTC()
	windowResponse := testutils.BuildTestWriteMessages(int64(15), testStartTime, nil)

	tests := []struct {
		name          string
		buffers       []isb.BufferWriter
		throwError    bool
		expectedCount int
		responses     []isb.Message
	}{
		{
			name: "test-discard-latest",
			buffers: []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("buffer1-1", 10, 0, simplebuffer.WithBufferFullWritingStrategy(dfv1.DiscardLatest)),
				simplebuffer.NewInMemoryBuffer("buffer1-2", 10, 1, simplebuffer.WithBufferFullWritingStrategy(dfv1.DiscardLatest))},
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError:    false,
			expectedCount: 15,
			responses:     windowResponse,
		},
		{
			name: "test-retry-until-success",
			buffers: []isb.BufferWriter{simplebuffer.NewInMemoryBuffer("buffer2-1", 10, 0, simplebuffer.WithBufferFullWritingStrategy(dfv1.RetryUntilSuccess)),
				simplebuffer.NewInMemoryBuffer("buffer2-2", 10, 0, simplebuffer.WithBufferFullWritingStrategy(dfv1.RetryUntilSuccess))},
			// should throw context closed error as we keep retrying writing until context is cancelled
			throwError:    true,
			expectedCount: 0,
			responses:     windowResponse,
		},
	}
	for _, value := range tests {
		t.Run(value.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			toBuffer := map[string][]isb.BufferWriter{
				"buffer": value.buffers,
			}

			buffers := make([]string, 0)
			for k := range toBuffer {
				buffers = append(buffers, k)
			}

			whereto := &forwardTest{
				buffers: buffers,
			}

			mngr := &ProcessAndForward{
				toBuffers:      toBuffer,
				whereToDecider: whereto,
				log:            logging.FromContext(ctx),
				pipelineName:   testPipelineName,
				vertexName:     "testVertex",
				vertexReplica:  0,
			}

			writeOffsets, _ := mngr.writeToBuffer(ctx, "buffer", 0, value.responses)
			assert.Equal(t, value.expectedCount, len(writeOffsets))
		})
	}
}
