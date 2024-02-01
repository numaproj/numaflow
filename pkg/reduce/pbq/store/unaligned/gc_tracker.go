package unaligned

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/window"
)

const (
	eventsFilePrefix  = "events-file"
	currentEventsFile = "current" + "-" + eventsFilePrefix
)

type gcEventsTracker struct {
	eventsPath       string        // dir path to the events file
	currEventsFile   *os.File      // current events file to write to
	eventsBufWriter  *bufio.Writer // buffer writer for the events file
	prevSyncedTime   time.Time     // previous synced time
	syncDuration     time.Duration // sync duration
	encoder          *encoder      // encoder for the events file
	rotationDuration time.Duration // rotation duration
	stopSignal       chan struct{} // stopSignal channel
	doneCh           chan struct{} // done channel
	mu               sync.Mutex
}

// NewGCEventsTracker returns a new GC tracker instance
func NewGCEventsTracker(ctx context.Context, opts ...GCTrackerOption) (GCEventsTracker, error) {
	tracker := &gcEventsTracker{
		currEventsFile:  nil,
		eventsBufWriter: nil,
		prevSyncedTime:  time.Now(),
		encoder:         newEncoder(),
		mu:              sync.Mutex{},
		stopSignal:      make(chan struct{}),
		doneCh:          make(chan struct{}),
	}

	for _, opt := range opts {
		opt(tracker)
	}

	// open the events file
	err := tracker.openEventsFile()

	// keep rotating the file
	go tracker.keepRotating(ctx)

	return tracker, err
}

// keepRotating keeps rotating the events file
func (g *gcEventsTracker) keepRotating(ctx context.Context) {
	rotationTimer := time.NewTicker(g.rotationDuration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-g.stopSignal:
			close(g.doneCh)
			return
		case <-rotationTimer.C:
			// rotate the file
			if err := g.rotateEventsFile(); err != nil {
				log.Println("Error while rotating the events file", zap.Error(err))
			}
		}
	}
}

// rotateEventsFile rotates the events file and updates the current events file
// with the new file
func (g *gcEventsTracker) rotateEventsFile() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.flushAndSync(); err != nil {
		return err
	}

	// close the current file
	if err := g.currEventsFile.Close(); err != nil {
		return err
	}

	// copy the events file and delete the current events file
	if err := os.Rename(g.currEventsFile.Name(), g.getEventsFilePath()); err != nil {
		log.Println("Error while renaming the events file", zap.Error(err))
	}
	return g.openEventsFile()
}

// getEventsFilePath returns the events file path
func (g *gcEventsTracker) getEventsFilePath() string {
	return filepath.Join(g.eventsPath, eventsFilePrefix+"-"+fmt.Sprintf("%d", time.Now().UnixMilli()))
}

// openEventsFile opens a new events file to write to
func (g *gcEventsTracker) openEventsFile() error {
	eventFilePath := filepath.Join(g.eventsPath, currentEventsFile)

	var err error
	if g.currEventsFile, err = os.OpenFile(eventFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644); err != nil {
		return err
	}

	// reset the data buffer writer
	if g.eventsBufWriter == nil {
		g.eventsBufWriter = bufio.NewWriter(g.currEventsFile)
	} else {
		g.eventsBufWriter.Reset(g.currEventsFile)
	}
	return nil
}

// TrackGCEvent tracks the GC event
func (g *gcEventsTracker) TrackGCEvent(window window.TimedWindow) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	dms := &deletionMessage{
		St:   window.StartTime().UnixMilli(),
		Et:   window.EndTime().UnixMilli(),
		Slot: window.Slot(),
		Key:  strings.Join(window.Keys(), dfv1.KeysDelimitter),
	}

	// encode and write the deletion message
	dbytes, err := g.encoder.encodeDeletionEvent(dms)
	if err != nil {
		return err
	}

	if err := binary.Write(g.eventsBufWriter, binary.LittleEndian, dbytes); err != nil {
		return err
	}

	// sync the file if the sync duration is elapsed
	if time.Since(g.prevSyncedTime) >= g.syncDuration {
		if err := g.flushAndSync(); err != nil {
			return err
		}
	}

	return nil
}

func (g *gcEventsTracker) flushAndSync() error {
	if err := g.eventsBufWriter.Flush(); err != nil {
		return err
	}

	g.prevSyncedTime = time.Now()
	return g.currEventsFile.Sync()
}

// Close closes the tracker by flushing and syncing the current events file
func (g *gcEventsTracker) Close() error {
	// stop the rotation by closing the stopSignal channel
	// and wait for the done channel to be closed
	close(g.stopSignal)
	<-g.doneCh

	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.flushAndSync(); err != nil {
		return err
	}

	if err := g.currEventsFile.Close(); err != nil {
		return err
	}

	// rename the current events file to the events file
	if err := os.Rename(filepath.Join(g.eventsPath, currentEventsFile), g.getEventsFilePath()); err != nil {
		return err
	}

	return nil
}
