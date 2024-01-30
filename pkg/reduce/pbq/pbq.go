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
	"fmt"
	"strconv"
	"sync"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/aligned"
	"github.com/numaproj/numaflow/pkg/window"
)

// PBQ Buffer queue which is backed with a persisted store, each partition
// will have a PBQ associated with it
type PBQ struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	store         aligned.Store
	output        chan *window.TimedWindowRequest
	cob           bool // cob to avoid panic in case writes happen after close of book
	PartitionID   partition.ID
	options       *options
	manager       *Manager
	windowType    window.Type
	log           *zap.SugaredLogger
	mu            sync.Mutex
}

var _ ReadWriteCloser = (*PBQ)(nil)

// Write accepts a window request and writes it to the PBQ, only the isb message is written to the store.
// The other metadata like operation etc are recomputed from WAL.
// request can never be nil.
func (p *PBQ) Write(ctx context.Context, request *window.TimedWindowRequest) error {
	// if cob we should return
	if p.cob {
		p.log.Errorw("Failed to write request to pbq, pbq is closed", zap.Any("ID", p.PartitionID), zap.Any("request", request))
		return nil
	}

	// if the window operation is delete, we should close the output channel and return
	if request.Operation == window.Delete {
		p.CloseOfBook()
		return nil
	}

	var writeErr error
	// we need context to get out of blocking write
	select {
	case p.output <- request:
		switch request.Operation {
		case window.Open, window.Append, window.Expand:
			// this is a blocking call, ctx.Done() will be ignored.
			if p.windowType == window.Unaligned {
				return nil
			}
			writeErr = p.store.Write(request.ReadMessage)
		case window.Close, window.Merge:
		// these do not have request.ReadMessage, only metadata fields are used
		default:
			return fmt.Errorf("unknown request.Operation, %v", request.Operation)
		}
	case <-ctx.Done():
		// closing the output channel will not cause panic, since its inside select case
		// ctx.Done implicitly means write hasn't succeeded.
		close(p.output)
		writeErr = ctx.Err()
	}
	pbqChannelSize.With(map[string]string{
		metrics.LabelVertex:             p.vertexName,
		metrics.LabelPipeline:           p.pipelineName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(p.vertexReplica)),
	}).Set(float64(len(p.output)))
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
	p.mu.Lock()
	defer p.mu.Unlock()
	// we need a nil check because PBQ.GC could have been invoked before close
	if p.store != nil {
		return p.store.Close()
	}
	return nil
}

// ReadCh exposes read channel to read the window requests from the PBQ
// close on read channel indicates COB
func (p *PBQ) ReadCh() <-chan *window.TimedWindowRequest {
	return p.output
}

// GC cleans up the PBQ and also the store associated with it. GC is invoked after the Reader (ProcessAndForward) has
// finished forwarding the output to ISB.
func (p *PBQ) GC() error {
	// we need a lock because Close() and PBQ.GC() can be invoked simultaneously
	// by shutdown routine(pbq.GC in case of ctx close) and pnf(pbq.Close after forwarding the result)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store = nil
	return p.manager.deregister(p.PartitionID)
}

// replayRecordsFromStore replays store messages when replay flag is set during start up time. It replays by reading from
// the store and writing to the PBQ channel.
// FIXME: works only for fixed and sliding window
func (p *PBQ) replayRecordsFromStore(ctx context.Context) {
	size := p.options.readBatchSize
readLoop:
	for {
		readMessages, eof, err := p.store.Read(size)
		if err != nil {
			p.log.Errorw("Error while replaying records from store", zap.Any("ID", p.PartitionID), zap.Error(err))
		}
		for _, msg := range readMessages {
			// FIXME: support for session window
			var w window.TimedWindow
			if p.windowType == window.Aligned {
				w = window.NewAlignedTimedWindow(p.PartitionID.Start, p.PartitionID.End, p.PartitionID.Slot)
			} else {
				p.log.Errorw("unAligned window strategy not supported", zap.Any("ID", p.PartitionID), zap.Any("strategy", p.windowType))
			}
			// select to avoid infinite blocking while writing to output channel
			select {
			case p.output <- &window.TimedWindowRequest{
				ReadMessage: msg,
				Operation:   window.Append,
				Windows:     []window.TimedWindow{w},
				ID:          &p.PartitionID,
			}:
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
