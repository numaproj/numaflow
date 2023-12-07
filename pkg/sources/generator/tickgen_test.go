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

package generator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/sources/forward/applier"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type myForwardToAllTest struct {
}

func (f myForwardToAllTest) WhereTo(_ []string, _ []string) ([]forwarder.VertexBuffer, error) {
	return []forwarder.VertexBuffer{{
		ToVertexName:         "writer",
		ToVertexPartitionIdx: 0,
	}}, nil
}

func TestRead(t *testing.T) {
	dest := simplebuffer.NewInMemoryBuffer("writer", 100, 0, simplebuffer.WithReadTimeOut(10*time.Second))
	ctx := context.Background()
	vertex := &dfv1.Vertex{
		ObjectMeta: v1.ObjectMeta{
			Name: "memGen",
		},
		Spec: dfv1.VertexSpec{
			PipelineName: "testPipeline",
			AbstractVertex: dfv1.AbstractVertex{
				Name: "testVertex",
				Source: &dfv1.Source{
					Generator: &dfv1.GeneratorSource{},
				},
			},
		},
	}
	m := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "TestRead",
		Replica:  0,
	}

	publishWMStore, _ := store.BuildNoOpWatermarkStore()
	toBuffers := map[string][]isb.BufferWriter{
		"writer": {dest},
	}

	fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(toBuffers)
	toVertexWmStores := map[string]store.WatermarkStore{
		"writer": publishWMStore,
	}
	mgen, err := NewMemGen(m, toBuffers, myForwardToAllTest{}, applier.Terminal, fetchWatermark, toVertexWmStores, publishWMStore, wmb.NewIdleManager(len(toBuffers)))
	assert.NoError(t, err)
	_ = mgen.Start()

	msgs, err := dest.Read(ctx, 5)

	mgen.Stop()

	assert.Nil(t, err)
	assert.Equal(t, 5, len(msgs))

	// wait for the context to be completely stopped.
	for {
		_, ok := <-mgen.(*memGen).srcChan
		if !ok {
			break
		}
	}

}

// Intention of this test is test the wiring for stop.
// initially a set of records will be written and subsequently
// when stop is invoked, we make sure that we have infact read all the messages
// before the consumer is shut down.
func TestStop(t *testing.T) {
	// for use by the buffer reader on the other side of the stream
	ctx := context.Background()

	// default rpu is 5. set the test to run for 2 ticks.
	dest := simplebuffer.NewInMemoryBuffer("writer", 10, 0)
	vertex := &dfv1.Vertex{
		ObjectMeta: v1.ObjectMeta{
			Name: "memGen",
		},
		Spec: dfv1.VertexSpec{
			PipelineName: "testPipeline",
			AbstractVertex: dfv1.AbstractVertex{
				Name: "testVertex",
				Source: &dfv1.Source{
					Generator: &dfv1.GeneratorSource{},
				},
			},
		},
	}
	m := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "TestRead",
		Replica:  0,
	}
	publishWMStore, _ := store.BuildNoOpWatermarkStore()
	toBuffers := map[string][]isb.BufferWriter{
		"writer": {dest},
	}

	fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressorsFromBufferMap(toBuffers)
	toVertexWmStores := map[string]store.WatermarkStore{
		"writer": publishWMStore,
	}
	mgen, err := NewMemGen(m, toBuffers, myForwardToAllTest{}, applier.Terminal, fetchWatermark, toVertexWmStores, publishWMStore, wmb.NewIdleManager(len(toBuffers)))
	assert.NoError(t, err)
	stop := mgen.Start()

	starttime := time.Now()
	// wait for some messages
	for {
		if !dest.IsEmpty() {
			break
		}
	}
	duration := time.Since(starttime)

	t.Logf("took [%v] millis to detect IsFull ", duration.Milliseconds())

	// initiate shutdown
	mgen.Stop()

	// reader should have drained all the messages
	// try to read everything
	// Read is blocking and we dont know how many...
	msgsread := 0

	for {
		select {
		// either of these cases imply that the forwarder has exited.
		case <-stop:
			t.Logf("msgs read [%v]", msgsread)
			assert.Greater(t, msgsread, 0)
			return
		default:
			msgs, rerr := dest.Read(ctx, 1)
			assert.Nil(t, rerr)
			if len(msgs) > 0 {
				_ = dest.Ack(ctx, []isb.Offset{msgs[0].ReadOffset})
			}
		}
		msgsread += 1
	}
}

func TestTimeParsing(t *testing.T) {
	rbytes := recordGenerator(8, nil, time.Now().UnixNano())
	parsedtime := parseTime(rbytes)
	assert.True(t, parsedtime > 0)
}

func TestUnparseableTime(t *testing.T) {
	rbytes := []byte("this is unparseable as json")
	parsedtime := parseTime(rbytes)
	assert.True(t, parsedtime == 0)
}

func TestTimeForValidTime(t *testing.T) {
	nanotime := time.Now().UnixNano()
	parsedtime := timeFromNanos(nanotime)
	assert.Equal(t, nanotime, parsedtime.UnixNano())
}

func TestTimeForInvalidTime(t *testing.T) {
	nanotime := int64(-1)
	parsedtime := timeFromNanos(nanotime)
	assert.True(t, parsedtime.UnixNano() > 0)
}

func TestWatermark(t *testing.T) {
	// TODO: fix test
	t.SkipNow()
	// for use by the buffer reader on the other side of the stream
	ctx := context.Background()

	dest := simplebuffer.NewInMemoryBuffer("writer", 1000, 0)
	vertex := &dfv1.Vertex{
		ObjectMeta: v1.ObjectMeta{
			Name: "memGen",
		},
		Spec: dfv1.VertexSpec{
			PipelineName: "testPipeline",
			AbstractVertex: dfv1.AbstractVertex{
				Name: "testVertex",
				Source: &dfv1.Source{
					Generator: &dfv1.GeneratorSource{},
				},
			},
		},
	}
	m := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "TestRead",
		Replica:  0,
	}
	toBuffers := map[string][]isb.BufferWriter{
		"writer": {dest},
	}

	publishWMStore, _ := store.BuildNoOpWatermarkStore()
	mgen, err := NewMemGen(m, toBuffers, myForwardToAllTest{}, applier.Terminal, nil, nil, publishWMStore, nil)
	assert.NoError(t, err)
	stop := mgen.Start()

	starttime := time.Now()
	// wait for some messages
	for {
		if dest.IsFull() {
			break
		}
	}
	duration := time.Since(starttime)

	t.Logf("took [%v] millis to detect IsFull ", duration.Milliseconds())

	rctx, rcf := context.WithCancel(ctx)

	// initiate shutdown
	mgen.Stop()
	rcf()

	// reader should have drained all the messages
	// try to read everything
	// Read is blocking and we dont know how many...
	msgsread := 0

	for {
		select {
		// either of these cases imply that the forwarder has exited.
		case <-stop:
			t.Logf("msgs read [%v]", msgsread)
			assert.Greater(t, msgsread, 0)
			return
		default:
			msgs, rerr := dest.Read(rctx, 1)
			assert.Nil(t, rerr)
			if len(msgs) > 0 {
				_ = dest.Ack(rctx, []isb.Offset{msgs[0].ReadOffset})
			}
		}
		msgsread += 1
	}
}
