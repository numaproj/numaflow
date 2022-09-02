package pbq

import (
	"context"
	"errors"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"go.uber.org/zap"
)

var COBErr error = errors.New("error while writing to pbq, pbq is closed")

type PBQ struct {
	store       store.Store
	output      chan *isb.Message
	cob         bool // cob to avoid panic in case writes happen after close of book
	partitionID string
	options     *Options
	manager     *Manager
	log         *zap.SugaredLogger
}

// Write writes message to pbq and persistent store
// We don't need a context here as this is invoked for every message.
func (p *PBQ) Write(ctx context.Context, message *isb.Message) (writeErr error) {
	// if cob we should return
	if p.cob {
		p.log.Errorw("failed to write message to pbq, pbq is closed", zap.Any("partitionID", p.partitionID), zap.Any("header", message.Header))
		writeErr = COBErr
		return
	}
	// we need context to get out of blocking write
	select {
	case p.output <- message:
		writeErr = p.store.Write(message)
		return
	case <-ctx.Done():
		// closing the output channel will not cause panic, since its inside select case
		close(p.output)
		writeErr = p.store.Close()
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
	closeErr = p.store.Close()
	return
}

// ReadCh exposes read channel to read messages from PBQ
// close on read channel indicates COB
func (p *PBQ) ReadCh() <-chan *isb.Message {
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
	gcErr = p.store.GC()
	p.store = nil
	p.manager.Deregister(p.partitionID)
	return
}

// ReplayRecordsFromStore replays store messages when replay flag is set during start up time
func (p *PBQ) ReplayRecordsFromStore(ctx context.Context) {
	size := p.options.readBatchSize
readLoop:
	for {
		readMessages, eof, err := p.store.Read(int64(size))
		if err != nil {
			p.log.Errorw("error while replaying records from store", zap.Any("partitionID", p.partitionID), zap.Error(err))
		}
		for _, msg := range readMessages {
			// select to avoid infinite blocking while writing to output channel
			select {
			case p.output <- msg:
			case <-ctx.Done():
				break readLoop
			}
		}
		// after replaying all the messages from store, unset replay flag
		if eof {
			break
		}
	}
}
