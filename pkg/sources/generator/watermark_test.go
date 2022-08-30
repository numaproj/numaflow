package generator

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/simplebuffer"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWatermark(t *testing.T) {
	// TODO: fix test
	t.SkipNow()
	// for use by the buffer reader on the other side of the stream
	ctx := context.Background()

	_ = os.Setenv(dfv1.EnvWatermarkOn, "true")
	defer func() { _ = os.Unsetenv(dfv1.EnvWatermarkOn) }()

	dest := simplebuffer.NewInMemoryBuffer("writer", 1000)
	vertex := &dfv1.Vertex{ObjectMeta: v1.ObjectMeta{
		Name: "memgen",
	}}
	m := &dfv1.VertexInstance{
		Vertex:   vertex,
		Hostname: "TestRead",
		Replica:  0,
	}
	publishWMStore := generic.BuildPublishWMStores(noop.NewKVNoOpStore(), noop.NewKVNoOpStore())
	mgen, err := NewMemGen(m, 1, 8, time.Millisecond, []isb.BufferWriter{dest}, nil, nil, publishWMStore)
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
