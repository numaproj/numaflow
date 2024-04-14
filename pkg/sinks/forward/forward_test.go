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

package forward

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

const (
	testPipelineName = "testPipeline"
	testVertexName   = "testVertex"
)

var (
	testStartTime = time.Unix(1636470000, 0).UTC()
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// testForwardFetcher is for data_forward_test.go only
type testForwardFetcher struct {
}

// ComputeWatermark uses current time as the watermark because we want to make sure
// the test publisher is publishing watermark
func (t *testForwardFetcher) ComputeWatermark(_ isb.Offset, _ int32) wmb.Watermark {
	return t.getWatermark()
}

func (t *testForwardFetcher) getWatermark() wmb.Watermark {
	return wmb.Watermark(testStartTime)
}

func (t *testForwardFetcher) ComputeHeadIdleWMB(int32) wmb.WMB {
	// won't be used
	return wmb.WMB{}
}

func TestNewDataForward(t *testing.T) {
	var (
		testName        = "forward"
		batchSize int64 = 10
	)

	t.Run(testName+"_basic", func(t *testing.T) {
		metricsReset()
		// set the buffer size to be 5 * batchSize, so we have enough space for testing
		fromStep := simplebuffer.NewInMemoryBuffer("from", 5*batchSize, 0)
		// as of now, all of our sinkers have only 1 sinkWriter
		to1 := simplebuffer.NewInMemoryBuffer(testVertexName, 5*batchSize, 0)
		toSteps := map[string][]isb.BufferWriter{
			testVertexName: {to1},
		}

		vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
			PipelineName: testPipelineName,
			AbstractVertex: dfv1.AbstractVertex{
				Name: testVertexName,
			},
		}}

		vertexInstance := &dfv1.VertexInstance{
			Vertex:  vertex,
			Replica: 0,
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		writeMessages := testutils.BuildTestWriteMessages(4*batchSize, testStartTime, nil)

		_, _ = generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
		fetchWatermark := &testForwardFetcher{}
		f, err := NewDataForward(vertexInstance, fromStep, to1, fetchWatermark)

		assert.NoError(t, err)
		assert.False(t, to1.IsFull())
		assert.True(t, to1.IsEmpty())

		stopped := f.Start()
		// write some data
		_, errs := fromStep.Write(ctx, writeMessages[0:batchSize])
		assert.Equal(t, make([]error, batchSize), errs)

		testReadBatchSize := int(batchSize / 2)
		msgs := to1.GetMessages(testReadBatchSize)
		for ; msgs[testReadBatchSize-1].ID == ""; msgs = to1.GetMessages(testReadBatchSize) {
			select {
			case <-ctx.Done():
				if ctx.Err() == context.DeadlineExceeded {
					t.Fatal("expected to have messages in to buffer", ctx.Err())
				}
			default:
				time.Sleep(1 * time.Millisecond)
			}
		}
		// read some data
		readMessages, err := to1.Read(ctx, int64(testReadBatchSize))
		assert.NoError(t, err, "expected no error")
		assert.Len(t, readMessages, testReadBatchSize)
		for i := 0; i < testReadBatchSize; i++ {
			assert.Equal(t, []interface{}{writeMessages[i].MessageInfo}, []interface{}{readMessages[i].MessageInfo})
			assert.Equal(t, []interface{}{writeMessages[i].Kind}, []interface{}{readMessages[i].Kind})
			assert.Equal(t, []interface{}{writeMessages[i].Keys}, []interface{}{readMessages[i].Keys})
			assert.Equal(t, []interface{}{writeMessages[i].Body}, []interface{}{readMessages[i].Body})
		}

		// iterate to see whether metrics will eventually succeed
		err = validateMetrics(batchSize)
		for err != nil {
			select {
			case <-ctx.Done():
				t.Errorf("failed waiting metrics collection to succeed [%s] (%s)", ctx.Err(), err)
			default:
				time.Sleep(10 * time.Millisecond)
				err = validateMetrics(batchSize)

			}
		}

		// write some data
		_, errs = fromStep.Write(ctx, writeMessages[batchSize:4*batchSize])
		assert.Equal(t, make([]error, 3*batchSize), errs)

		f.Stop()
		time.Sleep(1 * time.Millisecond)
		// only for shutdown will work as from buffer is not empty
		f.ForceStop()

		<-stopped

	})
}

// TestWriteToBuffer tests two BufferFullWritingStrategies: 1. discarding the latest message and 2. retrying writing until context is cancelled.
func TestWriteToBuffer(t *testing.T) {
	tests := []struct {
		name       string
		batchSize  int64
		strategy   dfv1.BufferFullWritingStrategy
		throwError bool
	}{
		{
			name:      "test-discard-latest",
			batchSize: 10,
			strategy:  dfv1.DiscardLatest,
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError: false,
		},
		{
			name:      "test-retry-until-success",
			batchSize: 10,
			strategy:  dfv1.RetryUntilSuccess,
			// should throw context closed error as we keep retrying writing until context is cancelled
			throwError: true,
		},
		{
			name:      "test-discard-latest",
			batchSize: 1,
			strategy:  dfv1.DiscardLatest,
			// should not throw any error as we drop messages and finish writing before context is cancelled
			throwError: false,
		},
		{
			name:      "test-retry-until-success",
			batchSize: 1,
			strategy:  dfv1.RetryUntilSuccess,
			// should throw context closed error as we keep retrying writing until context is cancelled
			throwError: true,
		},
	}
	for _, value := range tests {
		t.Run(value.name, func(t *testing.T) {
			fromStep := simplebuffer.NewInMemoryBuffer("from", 5*value.batchSize, 0)
			buffer := simplebuffer.NewInMemoryBuffer("to1", value.batchSize, 0, simplebuffer.WithBufferFullWritingStrategy(value.strategy))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
				PipelineName: "testPipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "to1",
				},
			}}

			vertexInstance := &dfv1.VertexInstance{
				Vertex:  vertex,
				Replica: 0,
			}

			fetchWatermark := &testForwardFetcher{}

			f, err := NewDataForward(vertexInstance, fromStep, buffer, fetchWatermark)

			assert.NoError(t, err)
			assert.False(t, buffer.IsFull())
			assert.True(t, buffer.IsEmpty())

			stopped := f.Start()
			go func() {
				for !buffer.IsFull() {
					select {
					case <-ctx.Done():
						logging.FromContext(ctx).Fatalf("not full, %s", ctx.Err())
					default:
						time.Sleep(1 * time.Millisecond)
					}
				}
				// stop will cancel the context
				f.Stop()
			}()

			// try to write to buffer after it is full.
			var messageToStep []isb.Message
			writeMessages := testutils.BuildTestWriteMessages(4*value.batchSize, testStartTime, nil)
			messageToStep = append(messageToStep, writeMessages[0:value.batchSize+1]...)
			_, err = f.writeToSink(ctx, buffer, messageToStep)

			assert.Equal(t, value.throwError, err != nil)
			if value.throwError {
				// assert the number of failed messages
				assert.True(t, strings.Contains(err.Error(), "with failed messages:1"))
			}
			<-stopped
		})
	}
}

func validateMetrics(batchSize int64) (err error) {
	metadata := `
		# HELP forwarder_data_read_total Total number of Data Messages Read
		# TYPE forwarder_data_read_total counter
		`
	expected := `
		forwarder_data_read_total{partition_name="from",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="Sink"} ` + fmt.Sprintf("%f", float64(batchSize)) + `
	`

	err = testutil.CollectAndCompare(metrics.ReadDataMessagesCount, strings.NewReader(metadata+expected), "forwarder_data_read_total")
	if err != nil {
		return err
	}

	writeMetadata := `
		# HELP forwarder_write_total Total number of Messages Written
		# TYPE forwarder_write_total counter
		`
	writeExpected := `
		forwarder_write_total{partition_name="testVertex",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="Sink"} ` + fmt.Sprintf("%d", batchSize) + `
	`
	err = testutil.CollectAndCompare(metrics.WriteMessagesCount, strings.NewReader(writeMetadata+writeExpected), "forwarder_write_total")
	if err != nil {
		return err
	}

	ackMetadata := `
		# HELP forwarder_ack_total Total number of Messages Acknowledged
		# TYPE forwarder_ack_total counter
		`
	ackExpected := `
		forwarder_ack_total{partition_name="from",pipeline="testPipeline",replica="0",vertex="testVertex",vertex_type="Sink"} ` + fmt.Sprintf("%d", batchSize) + `
	`

	err = testutil.CollectAndCompare(metrics.AckMessagesCount, strings.NewReader(ackMetadata+ackExpected), "forwarder_ack_total")
	if err != nil {
		return err
	}

	return nil
}

func metricsReset() {
	metrics.ReadDataMessagesCount.Reset()
	metrics.WriteMessagesCount.Reset()
	metrics.AckMessagesCount.Reset()
}
