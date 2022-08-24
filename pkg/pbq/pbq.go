package pbq

import (
	"context"
	"errors"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

var EOFError error = errors.New("error while reading, EOF")
var ContextClosedError error = errors.New("context was cob")

type PBQ struct {
	Store       store.Store
	output      chan *isb.Message
	cob         bool // close of book to avoid panic in case writes happen after closeofbook
	partitionID string
	options     *store.Options
	isReplaying bool
	manager     *Manager
}

// NewPBQ accepts size and store and returns new PBQ
// TODO lets think if we need an error
func NewPBQ(partitionID string, persistentStore store.Store, pbqManager *Manager, options *store.Options) (*PBQ, error) {

	// output channel is buffered
	p := &PBQ{
		Store:       persistentStore,
		output:      make(chan *isb.Message, options.BufferSize),
		cob:         false,
		partitionID: partitionID,
		options:     options,
		manager:     pbqManager,
	}

	p.manager.Register()
	return p, nil
}

// WriteFromISB writes message to pbq and persistent store
// We don't need a context here as this is invoked for every message.
func (p *PBQ) WriteFromISB(message *isb.Message) (WriteErr error) {
	if p.cob {
		return errors.New("pbq is cob, cannot write the message")
	}
	p.output <- message
	WriteErr = p.Store.WriteToStore(message)
	return
}

//CloseOfBook closes output channel
func (p *PBQ) CloseOfBook() {
	p.cob = true
	close(p.output)
}

// ReadFromPBQ reads upto N messages (specified by size) from pbq
// if replay flag is set its reads messages from persisted store
func (p *PBQ) ReadFromPBQ(ctx context.Context, size int64) ([]*isb.Message, error) {
	if p.cob && len(p.output) == 0 {
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
			return pbqReadMessages, ctx.Err()
		default:
			// TODO select with timed wait
			msg, ok := <-p.output
			if msg != nil {
				pbqReadMessages = append(pbqReadMessages, msg)
			}
			// if cob and nothing more to read from output channel
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
	p.manager.Deregister()
	return
}

func (p *PBQ) SetIsReplaying(isReplaying bool) {
	p.isReplaying = isReplaying
}
