package logger

import (
	"context"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/isb/simplebuffer"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/stretchr/testify/assert"
)

var (
	testStartTime = time.Unix(1636470000, 0).UTC()
)

func TestToLog_Start(t *testing.T) {
	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	startTime := time.Unix(1636470000, 0)
	writeMessages := testutils.BuildTestWriteMessages(int64(20), startTime)

	vertex := &dfv1.Vertex{ObjectMeta: v1.ObjectMeta{
		Name: "sinks.logger",
	}}

	s, err := NewToLog(vertex, fromStep)
	assert.NoError(t, err)

	stopped := s.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	// write some data
	_, errs = fromStep.Write(ctx, writeMessages[5:20])
	assert.Equal(t, make([]error, 15), errs)

	s.Stop()

	<-stopped
}

type ForwardToAllVertex struct {
}

func (f ForwardToAllVertex) WhereTo(_ []byte) (string, error) {
	return dfv1.MessageKeyAll, nil
}

// TestToLog_ForwardToTwoVertex writes to 2 vertices and have a logger sinks attached to each vertex.
func TestToLog_ForwardToTwoVertex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	fromStep := simplebuffer.NewInMemoryBuffer("from", 25)
	to1 := simplebuffer.NewInMemoryBuffer("to1", 25)
	to2 := simplebuffer.NewInMemoryBuffer("to2", 25)

	// start the last vertex first
	// add 2 sinks per vertex
	vertex1 := &dfv1.Vertex{ObjectMeta: v1.ObjectMeta{
		Name: "sinks.logger1",
	}}
	vertex2 := &dfv1.Vertex{ObjectMeta: v1.ObjectMeta{
		Name: "sinks.logger2",
	}}
	logger1, _ := NewToLog(vertex1, to1)
	logger2, _ := NewToLog(vertex2, to2)
	logger1Stopped := logger1.Start()
	logger2Stopped := logger2.Start()

	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
		"to2": to2,
	}

	writeMessages := testutils.BuildTestWriteMessages(int64(20), testStartTime)
	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	f, err := forward.NewInterStepDataForward(vertex, fromStep, toSteps, forward.All, applier.Terminal, nil)
	assert.NoError(t, err)

	stopped := f.Start()
	// write some data
	_, errs := fromStep.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)
	f.Stop()
	<-stopped
	// downstream should be stopped only after upstream is stopped
	logger1.Stop()
	logger2.Stop()

	<-logger1Stopped
	<-logger2Stopped
}
