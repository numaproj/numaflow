package pbq

import (
	"context"
	"errors"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"
)

var COBErr error = errors.New("error while writing to pbq, pbq is closed")

type PBQ struct {
	Store       store.Store
	output      chan *isb.Message
	cob         bool // cob to avoid panic in case writes happen after close of book
	partitionID string
	options     *store.Options
	isReplaying bool
	manager     *Manager
	log         *zap.SugaredLogger
}

// NewPBQ accepts size and store and returns new PBQ
func NewPBQ(ctx context.Context, partitionID string, persistentStore store.Store, pbqManager *Manager, options *store.Options) (*PBQ, error) {

	// output channel is buffered to support bulk reads
	p := &PBQ{
		Store:       persistentStore,
		output:      make(chan *isb.Message, options.BufferSize()),
		cob:         false,
		partitionID: partitionID,
		options:     options,
		manager:     pbqManager,
		log:         logging.FromContext(ctx).With("PBQ", partitionID),
	}

	return p, nil
}

// WriteFromISB writes message to pbq and persistent store
// We don't need a context here as this is invoked for every message.
func (p *PBQ) WriteFromISB(ctx context.Context, message *isb.Message) (writeErr error) {
	// if we are replaying records from the store, writes should be blocked
	for p.isReplaying {
	}
	// if cob we should return
	if p.cob {
		p.log.Errorw("failed to write message to pbq, pbq is closed", zap.Any("partitionID", p.partitionID), zap.Any("header", message.Header))
		writeErr = COBErr
		return
	}
	// we need context to get out of blocking write
	select {
	case p.output <- message:
		writeErr = p.Store.WriteToStore(message)
		return
	case <-ctx.Done():
		// closing the output channel will not cause panic, since its inside select case
		close(p.output)
		writeErr = p.Store.Close()
	}
	return
}

//CloseOfBook closes output channel
func (p *PBQ) CloseOfBook() {
	close(p.output)
	p.cob = true
}

// CloseWriter is used by the writer to indicate close of context
// we should flush pending messages to store
func (p *PBQ) CloseWriter() (closeErr error) {
	closeErr = p.Store.Close()
	return
}

// ReadFromPBQCh exposes read channel to read messages from PBQ
// close on read channel indicates COB
func (p *PBQ) ReadFromPBQCh() <-chan *isb.Message {
	return p.output
}

// CloseReader is used by the Reader to indicate that it has finished
// consuming the data from output channel
func (p *PBQ) CloseReader() (closeErr error) {
	return
}

// GC is invoked after the Reader (ProcessAndForward) has finished
// forwarding the output to ISB.
func (p *PBQ) GC() (gcErr error) {
	gcErr = p.Store.GC()
	p.Store = nil
	p.manager.Deregister(p.partitionID)
	return
}

// SetIsReplaying sets the replay flag
func (p *PBQ) SetIsReplaying(ctx context.Context, isReplaying bool) {
	p.isReplaying = isReplaying
	go p.replayRecordsFromStore(ctx)
}

// replayRecordsFromStore replays store messages when replay flag is set during start up time
func (p *PBQ) replayRecordsFromStore(ctx context.Context) {
	size := p.options.ReadBatchSize()
readLoop:
	for {
		readMessages, eof, err := p.Store.ReadFromStore(int64(size))
		if err != nil {
			p.log.Errorw("error while replaying records from store", zap.Any("partitionID", p.partitionID), zap.Error(err))
		}
		for _, msg := range readMessages {
			// select to avoid infinite blocking while writing to output channel
			select {
			case p.output <- msg:
			case <-ctx.Done():
				p.isReplaying = false
				break readLoop
			}
		}
		// after replaying all the messages from store, unset replay flag
		if eof {
			p.isReplaying = false
			break
		}
	}
}
