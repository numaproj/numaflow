package pbq

import (
	"context"
	"errors"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"
	"time"
)

var EOF error = errors.New("error while reading, EOF")
var COBError error = errors.New("error while writing to pbq, pbq is closed")

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

	p.manager.Register(partitionID, p)
	return p, nil
}

// WriteFromISB writes message to pbq and persistent store
// We don't need a context here as this is invoked for every message.
func (p *PBQ) WriteFromISB(message *isb.Message) (WriteErr error) {
	if p.cob {
		p.log.Errorw("failed to write message to pbq, pbq is closed", zap.Any("partitionID", p.partitionID), zap.Any("header", message.Header))
		WriteErr = COBError
		return
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
	// if its close of book and there are no messages in output channel, return EOF
	if p.cob && len(p.output) == 0 {
		return []*isb.Message{}, EOF
	}
	var storeReadMessages []*isb.Message
	var err error

	// replay flag is set, so we will consider store messages
	if p.isReplaying {
		storeReadMessages, err = p.Store.ReadFromStore(size)
		// if store has no messages unset the replay flag
		if err == store.WriteStoreClosedError {
			p.isReplaying = false
		}
		return storeReadMessages, nil
	}
	var pbqReadMessages []*isb.Message
	chanDrained := false

	readTimer := time.NewTimer(time.Second * time.Duration(p.options.ReadTimeout()))
	defer readTimer.Stop()

	// read n(size) messages from pbq, if context is canceled we should return,
	// to avoid infinite blocking we have timer
	for i := int64(0); i < size; i++ {
		select {
		case <-ctx.Done():
			return pbqReadMessages, ctx.Err()
		default:
		readLoop:
			for {
				select {
				case msg, ok := <-p.output:
					if msg != nil {
						pbqReadMessages = append(pbqReadMessages, msg)
					}
					// channel is closed and all the messages are read
					if !ok {
						chanDrained = true
						goto exit
					}
					break readLoop
				case <-readTimer.C:
					// if timeout return
					goto exit
				}
			}
		}
	}

exit:
	if chanDrained {
		return pbqReadMessages, EOF
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
// forwarding the output to ISB.
func (p *PBQ) GC() (GcErr error) {
	GcErr = p.Store.GC()
	p.Store = nil
	p.manager.Deregister(p.partitionID)
	return
}

// SetIsReplaying sets the replay flag
func (p *PBQ) SetIsReplaying(isReplaying bool) {
	p.isReplaying = isReplaying
}
