package pbq

import (
	"context"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/pbq/store/simplepbq"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPBQ_WriteFromISB(t *testing.T) {

	size := 10000
	memStore, err := simplepbq.NewMemoryStore(store.WithPbqStoreType("in-memory"), store.WithChannelSize(int64(size)))
	assert.NoError(t, err)

	ctx := context.Background()
	childCtx, _ := context.WithCancel(ctx)
	p, err := NewPBQ(childCtx, size, memStore)
	assert.NoError(t, err)

	startTime := time.Now()
	messages := testutils.BuildTestWriteMessages(int64(size), startTime)
	writeChan := p.WriteFromISB()
	for _, msg := range messages {
		writeChan <- &msg
	}
	//time.Sleep(1 * time.Second)
	p.CloseOfBook()
	assert.Equal(t, len(p.ReadFromPBQ()), size)

}

func TestPBQ_ReadFromPBQ(t *testing.T) {
	const size = 100
	memStore, err := simplepbq.NewMemoryStore(store.WithPbqStoreType("in-memory"), store.WithChannelSize(int64(size)))
	assert.NoError(t, err)

	ctx := context.Background()
	childCtx, _ := context.WithCancel(ctx)
	p, err := NewPBQ(childCtx, size, memStore)
	assert.NoError(t, err)

	startTime := time.Now()
	messages := testutils.BuildTestWriteMessages(int64(size), startTime)
	writeChan := p.WriteFromISB()
	for _, msg := range messages {
		writeChan <- &msg
	}
	p.CloseOfBook()

	var readMessages [size]isb.Message
	index := 0
	for msg := range p.ReadFromPBQ() {
		readMessages[index] = *msg
		index += 1
	}

	assert.Equal(t, len(messages), len(readMessages))
}
