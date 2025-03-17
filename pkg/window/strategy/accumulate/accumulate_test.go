package accumulate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/window"
)

func buildReadMessage(time time.Time, keys []string) *isb.ReadMessage {
	return &isb.ReadMessage{
		Message: isb.Message{
			Header: isb.Header{
				MessageInfo: isb.MessageInfo{
					EventTime: time,
				},
				Keys: keys,
			},
		},
	}
}

func TestAccumulate_AssignWindow(t *testing.T) {
	baseTime := time.Now()
	windower := NewWindower(&dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{
			Spec: dfv1.VertexSpec{
				PipelineName: "test-pipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "test-vertex",
				},
			},
		},
	}, time.Hour)

	readMsg := buildReadMessage(baseTime, []string{"key1"})
	windowRequests := windower.AssignWindows(readMsg)

	assert.Equal(t, 1, len(windowRequests))
	assert.Equal(t, baseTime, windowRequests[0].ReadMessage.EventTime)
	assert.Equal(t, window.Append, windowRequests[0].Operation)
	assert.Equal(t, "key1", windowRequests[0].Windows[0].Keys()[0])
}

func TestAccumulate_InsertWindow(t *testing.T) {
	win := NewAccumulatorWindow([]string{"key1"})
	windower := NewWindower(&dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{
			Spec: dfv1.VertexSpec{
				PipelineName: "test-pipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "test-vertex",
				},
			},
		},
	}, time.Hour)

	accumulateWindower := windower.(*Windower)

	windower.InsertWindow(win)
	assert.Equal(t, 1, len(accumulateWindower.activeWindows))

	win2 := NewAccumulatorWindow([]string{"key2"})
	windower.InsertWindow(win2)
	assert.Equal(t, 2, len(accumulateWindower.activeWindows))
}

func TestAccumulate_CloseWindows(t *testing.T) {
	baseTime := time.Now()
	windower := NewWindower(&dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{
			Spec: dfv1.VertexSpec{
				PipelineName: "test-pipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "test-vertex",
				},
			},
		},
	}, time.Minute)

	readMsg := buildReadMessage(baseTime, []string{"key1"})
	windower.AssignWindows(readMsg)

	windowRequests := windower.CloseWindows(baseTime.Add(2 * time.Minute))
	assert.Equal(t, 1, len(windowRequests))
	assert.Equal(t, window.Close, windowRequests[0].Operation)
	assert.Equal(t, "key1", windowRequests[0].Windows[0].Keys()[0])
}

func TestAccumulate_DeleteClosedWindow(t *testing.T) {
	baseTime := time.Now()
	windower := NewWindower(&dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{
			Spec: dfv1.VertexSpec{
				PipelineName: "test-pipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "test-vertex",
				},
			},
		},
	}, time.Minute)

	readMsgOne := buildReadMessage(baseTime, []string{"key1"})
	windower.AssignWindows(readMsgOne)
	readMsgTwo := buildReadMessage(baseTime.Add(time.Minute), []string{"key2"})
	windower.AssignWindows(readMsgTwo)

	windower.DeleteClosedWindow(&accumulatorWindow{endTime: baseTime.Add(time.Second), keys: []string{"key1"}})
	oldestWindowEndTime := windower.OldestWindowEndTime()
	assert.Equal(t, baseTime.Add(time.Minute), oldestWindowEndTime)
}

func TestAccumulate_OldestWindowEndTime(t *testing.T) {
	baseTime := time.Now()
	windower := NewWindower(&dfv1.VertexInstance{
		Vertex: &dfv1.Vertex{
			Spec: dfv1.VertexSpec{
				PipelineName: "test-pipeline",
				AbstractVertex: dfv1.AbstractVertex{
					Name: "test-vertex",
				},
			},
		},
	}, time.Minute)

	readMsg1 := buildReadMessage(baseTime, []string{"key1"})
	readMsg2 := buildReadMessage(baseTime.Add(time.Minute), []string{"key2"})
	windower.AssignWindows(readMsg1)
	windower.AssignWindows(readMsg2)

	assert.Equal(t, baseTime, windower.OldestWindowEndTime())
}
