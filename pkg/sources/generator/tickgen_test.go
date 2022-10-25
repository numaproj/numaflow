package generator

import (
	"context"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/isb/stores/simplebuffer"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/stretchr/testify/assert"
)

func TestRead(t *testing.T) {
	dest := simplebuffer.NewInMemoryBuffer("writer", 20)
	ctx := context.Background()
	vertex := &dfv1.Vertex{
		ObjectMeta: v1.ObjectMeta{
			Name: "memgen",
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

	publishWMStore := store.BuildWatermarkStore(noop.NewKVNoOpStore(), noop.NewKVNoOpStore())
	toSteps := map[string]isb.BufferWriter{
		"writer": dest,
	}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	mgen, err := NewMemGen(m, []isb.BufferWriter{dest}, fetchWatermark, publishWatermark, publishWMStore)
	assert.NoError(t, err)
	_ = mgen.Start()

	time.Sleep(time.Millisecond)
	msgs, err := dest.Read(ctx, 5)

	assert.Nil(t, err)
	assert.Equal(t, 5, len(msgs))
}

// Intention of this test is test the wiring for stop.
// initially a set of records will be written and subsequently
// when stop is invoked, we make sure that we have infact read all the messages
// before the consumer is shut down.
func TestStop(t *testing.T) {
	// for use by the buffer reader on the other side of the stream
	ctx := context.Background()

	dest := simplebuffer.NewInMemoryBuffer("writer", 100)
	vertex := &dfv1.Vertex{
		ObjectMeta: v1.ObjectMeta{
			Name: "memgen",
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
	publishWMStore := store.BuildWatermarkStore(noop.NewKVNoOpStore(), noop.NewKVNoOpStore())
	toSteps := map[string]isb.BufferWriter{
		"writer": dest,
	}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	mgen, err := NewMemGen(m, []isb.BufferWriter{dest}, fetchWatermark, publishWatermark, publishWMStore)
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

func TestTimeParsing(t *testing.T) {
	rbytes := recordGenerator(8)
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
	parsedtime := timefromNanos(nanotime)
	assert.Equal(t, nanotime, parsedtime.UnixNano())
}

func TestTimeForInvalidTime(t *testing.T) {
	nanotime := int64(-1)
	parsedtime := timefromNanos(nanotime)
	assert.True(t, parsedtime.UnixNano() > 0)
}
