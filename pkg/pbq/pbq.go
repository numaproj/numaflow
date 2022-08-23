package pbq

import (
	"context"
	"errors"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

var EOFError error = errors.New("error while reading, EOF")
var ContextClosedError error = errors.New("context was closed")

type PBQ struct {
	Store       store.Store
	output      chan *isb.Message
	closed      bool // closed to avoid panic in case writes happen after closeofbook
	partitionID string
	options     *store.Options
	isReplaying bool
}

//NewPBQ accepts size and store and returns new PBQ
func NewPBQ(partitionID string, persistentStore store.Store, options *store.Options) (*PBQ, error) {

	// output channel is buffered
	p := &PBQ{
		Store:       persistentStore,
		output:      make(chan *isb.Message, options.BufferSize),
		closed:      false,
		partitionID: partitionID,
		options:     options,
	}

	return p, nil
}

// WriteFromISB writes message to pbq and persistent store
// We don't need a context here as this is invoked for every message.
func (p *PBQ) WriteFromISB(message *isb.Message) (WriteErr error) {
	if p.closed {
		return errors.New("pbq is closed, cannot write the message")
	}
	p.output <- message
	WriteErr = p.Store.WriteToStore(message)
	return
}

//CloseOfBook closes output channel
func (p *PBQ) CloseOfBook() {
	p.closed = true
	close(p.output)
}

//ReadFromPBQ reads upto N messages from pbq
// if replay flag is set its reads messages from persisted store
func (p *PBQ) ReadFromPBQ(ctx context.Context, size int64) ([]*isb.Message, error) {
	if p.closed && len(p.output) == 0 {
		return []*isb.Message{}, EOFError
	}
	var storeReadMessages []*isb.Message
	var err error

	// replay flag is set, so we will consider store messages
	if p.isReplaying {
		storeReadMessages, err = p.Store.ReadFromStore(size)
		if err == nil {
			return storeReadMessages, nil
		}
		p.isReplaying = false
		return storeReadMessages, nil
	}

	var pbqReadMessages []*isb.Message
	chanDrained := false

readLoop:
	for i := int64(0); i < size; i++ {
		select {
		case <-ctx.Done():
			return pbqReadMessages, ContextClosedError
		default:
			msg, ok := <-p.output
			if msg != nil {
				pbqReadMessages = append(pbqReadMessages, msg)
			}
			if !ok && len(p.output) == 0 {
				chanDrained = true
				break readLoop
			}
		}
	}
	if chanDrained {
		return pbqReadMessages, EOFError
	}
	return pbqReadMessages, nil
}

// Close is used by the Reader to indicate that it has finished
// consuming the data from output channel
func (p *PBQ) Close() (CloseErr error) {
	CloseErr = p.Store.Close()
	return
}

// GC is invoked after the Reader (ProcessAndForward) has finished
// Forwarding the output to ISB.
func (p *PBQ) GC() (GcErr error) {
	GcErr = p.Store.GC()
	p.Store = nil
	return
}

func (p *PBQ) SetIsReplaying(isReplaying bool) {
	p.isReplaying = isReplaying
}
