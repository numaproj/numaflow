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
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/wal"
	"github.com/numaproj/numaflow/pkg/window"
)

const (
	currentEventsFile = "current-events-file"
	eventsFilePrefix  = "events-file"
)

type gCEventsTracker struct {
	eventsPath       string        // dir path to the events file
	currEventsFile   *os.File      // current events file to write to
	eventsBufWriter  *bufio.Writer // buffer writer for the events file
	prevSyncedTime   time.Time     // previous synced time
	syncDuration     time.Duration // sync duration
	encoder          *Encoder      // encoder for the events file
	rotationDuration time.Duration // rotation duration
	mu               sync.Mutex
}

// NewgCEventsTracker returns a new GC tracker instance
func NewgCEventsTracker(ctx context.Context, opts ...wal.GCTrackerOption) (GCEventsTracker, error) {
	tracker := &gCEventsTracker{
		currEventsFile:  nil,
		eventsBufWriter: nil,
		prevSyncedTime:  time.Now(),
		encoder:         NewEncoder(),
		mu:              sync.Mutex{},
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
func (g *gCEventsTracker) keepRotating(ctx context.Context) {
	rotationTimer := time.NewTimer(g.rotationDuration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-rotationTimer.C:
			// rotate the file
			// rotate the file
			if err := g.rotateEventsFile(); err != nil {
				log.Println("Error while rotating the events file", zap.Error(err))
			}
		}
	}
}

// rotateEventsFile rotates the events file and updates the current events file
// with the new file
func (g *gCEventsTracker) rotateEventsFile() error {
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
func (g *gCEventsTracker) getEventsFilePath() string {
	return filepath.Join(g.eventsPath, eventsFilePrefix+"-"+fmt.Sprintf("%d", time.Now().UnixMilli()))
}

// openEventsFile opens a new events file to write to
func (g *gCEventsTracker) openEventsFile() error {
	eventFilePath := filepath.Join(g.eventsPath, currentEventsFile)

	var err error
	if g.currEventsFile, err = os.OpenFile(eventFilePath, os.O_WRONLY|os.O_CREATE, 0644); err != nil {
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
func (g *gCEventsTracker) TrackGCEvent(window window.TimedWindow) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	deletionMessage := &DeletionMessage{
		St:   window.StartTime().UnixMilli(),
		Et:   window.EndTime().UnixMilli(),
		Slot: window.Slot(),
		Key:  strings.Join(window.Keys(), dfv1.KeysDelimitter),
	}

	// encode and write the deletion message
	dbytes, err := g.encoder.EncodeDeletionEvent(deletionMessage)
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

func (g *gCEventsTracker) flushAndSync() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.eventsBufWriter.Flush(); err != nil {
		return err
	}

	g.prevSyncedTime = time.Now()
	return g.currEventsFile.Sync()
}
