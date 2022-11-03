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
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/watermark/generic"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/stretchr/testify/assert"
)

type myShutdownTest struct {
}

func (s myShutdownTest) WhereTo(_ string) ([]string, error) {
	return []string{dfv1.MessageKeyAll}, nil
}

func (s myShutdownTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func TestInterStepDataForward_Stop(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startTime := time.Unix(1636470000, 0)
	writeMessages := testutils.BuildTestWriteMessages(int64(20), startTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myShutdownTest{}, myShutdownTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
	assert.NoError(t, err)
	stopped := f.Start()
	// write some data but buffer is not full even though we are not reading
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	f.Stop()
	// we cannot assert the result of IsShuttingDown because it might take a couple of iterations to be successful.
	_, _ = f.IsShuttingDown()
	<-stopped
}

func TestInterStepDataForward_ForceStop(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to", 10)
	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startTime := time.Unix(1636470000, 0)
	writeMessages := testutils.BuildTestWriteMessages(int64(20), startTime)

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := NewInterStepDataForward(vertex, fromStep, toSteps, myShutdownTest{}, myShutdownTest{}, fetchWatermark, publishWatermark, WithReadBatchSize(2))
	assert.NoError(t, err)
	stopped := f.Start()
	// write some data such that the fromBuffer can be empty, that is toBuffer gets full
	_, errs := fromStep.Write(ctx, writeMessages[0:20])
	assert.Equal(t, make([]error, 20), errs)

	f.Stop()
	canIShutdown, _ := f.IsShuttingDown()
	assert.Equal(t, true, canIShutdown)
	time.Sleep(1 * time.Millisecond)
	// only for canIShutdown will work as from buffer is not empty
	f.ForceStop()
	canIShutdown, err = f.IsShuttingDown()
	assert.NoError(t, err)
	assert.Equal(t, true, canIShutdown)

	<-stopped
}
