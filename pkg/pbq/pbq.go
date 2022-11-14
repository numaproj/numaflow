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

package pbq

import (
	"context"
	"errors"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"go.uber.org/zap"
)

var ErrCOB = errors.New("error while writing to pbq, pbq is closed")

// PBQ Buffer queue which is backed with a persisted store, each partition
// will have a PBQ associated with it
type PBQ struct {
	store       store.Store
	output      chan *isb.ReadMessage
	cob         bool // cob to avoid panic in case writes happen after close of book
	PartitionID partition.ID
	options     *options
	manager     *Manager
	log         *zap.SugaredLogger
}

var _ ReadWriteCloser = (*PBQ)(nil)

// Write writes message to pbq and persistent store
func (p *PBQ) Write(ctx context.Context, message *isb.ReadMessage) error {
	// if cob we should return
	if p.cob {
		p.log.Errorw("Failed to write message to pbq, pbq is closed", zap.Any("ID", p.PartitionID), zap.Any("header", message.Header))
		return ErrCOB
	}
	var writeErr error
	// we need context to get out of blocking write
	select {
	case p.output <- message:
		// this store.Write is an `inSync` flush (if need be). The performance will be very bad but the system is correct.
		// TODO: shortly in the near future we will move to async writes.
		writeErr = p.store.Write(message)
	case <-ctx.Done():
		// closing the output channel will not cause panic, since its inside select case
		// ctx.Done implicitly means write hasn't succeeded.
		close(p.output)
		writeErr = ctx.Err()
	}
	return writeErr
}

// CloseOfBook closes output channel
func (p *PBQ) CloseOfBook() {
	close(p.output)
	p.cob = true
}

// Close is used by the writer to indicate close of context
// we should flush pending messages to store
func (p *PBQ) Close() error {
	return p.store.Close()
}

// ReadCh exposes read channel to read messages from PBQ
// close on read channel indicates COB
func (p *PBQ) ReadCh() <-chan *isb.ReadMessage {
	return p.output
}

// GC cleans up the PBQ and also the store associated with it. GC is invoked after the Reader (ProcessAndForward) has
// finished forwarding the output to ISB.
func (p *PBQ) GC() error {
	err := p.store.GC()
	p.store = nil
	p.manager.deregister(p.PartitionID)
	return err
}

// replayRecordsFromStore replays store messages when replay flag is set during start up time. It replays by reading from
// the store and writing to the PBQ channel.
func (p *PBQ) replayRecordsFromStore(ctx context.Context) {
	size := p.options.readBatchSize
readLoop:
	for {
		readMessages, eof, err := p.store.Read(size)
		if err != nil {
			p.log.Errorw("Error while replaying records from store", zap.Any("ID", p.PartitionID), zap.Error(err))
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
