package unaligned

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/window"
)

const (
	eventsFilePrefix  = "events-file"
	currentEventsFile = "current" + "-" + eventsFilePrefix
)

type gcEventsTracker struct {
	eventsPath          string        // dir path to the events file
	currEventsFile      *os.File      // current events file to write to
	eventsBufWriter     *bufio.Writer // buffer writer for the events file
	prevSyncedTime      time.Time     // previous synced time
	syncDuration        time.Duration // sync duration
	encoder             *encoder      // encoder for the events file
	rotationDuration    time.Duration // rotation duration
	rotationEventsCount int           // rotation events count
	curEventsCount      int           // current events count
	fileCreationTime    time.Time     // file creation time
}

// NewGCEventsTracker returns a new GC tracker instance
func NewGCEventsTracker(ctx context.Context, opts ...GCTrackerOption) (GCEventsTracker, error) {
	tracker := &gcEventsTracker{
		syncDuration:        dfv1.DefaultGCTrackerSyncDuration,
		rotationDuration:    dfv1.DefaultGCTrackerRotationDuration,
		eventsPath:          dfv1.DefaultStoreEventsPath,
		currEventsFile:      nil,
		eventsBufWriter:     nil,
		prevSyncedTime:      time.Now(),
		encoder:             newEncoder(),
		rotationEventsCount: dfv1.DefaultGCTrackerRotationEventsCount,
		curEventsCount:      0,
		fileCreationTime:    time.Now(),
	}

	for _, opt := range opts {
		opt(tracker)
	}

	var err error
	// Create event dir if not exist
	if _, err = os.Stat(tracker.eventsPath); os.IsNotExist(err) {
		err = os.Mkdir(tracker.eventsPath, 0755)
		if err != nil {
			return nil, err
		}
	}

	// open the events file
	err = tracker.openEventsFile()

	return tracker, err
}

// rotateEventsFile rotates the events file and updates the current events file
// with the new file
func (g *gcEventsTracker) rotateEventsFile() error {
	defer func() {
		g.curEventsCount = 0
		g.fileCreationTime = time.Now()
	}()

	var err error
	if err = g.flushAndSync(); err != nil {
		return err
	}

	// close the current file
	if err = g.currEventsFile.Close(); err != nil {
		return err
	}

	// copy the events file and delete the current events file
	if err = os.Rename(g.currEventsFile.Name(), g.getEventsFilePath()); err != nil {
		return err
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

	if g.currEventsFile == nil {
		return fmt.Errorf("events file is not open")
	}

	dms := &deletionMessage{
		St:   window.StartTime().UnixMilli(),
		Et:   window.EndTime().UnixMilli(),
		Slot: window.Slot(),
		Key:  strings.Join(window.Keys(), dfv1.KeysDelimitter),
	}

	// encode and write the deletion message
	dBytes, err := g.encoder.encodeDeletionEvent(dms)
	if err != nil {
		return err
	}

	if err = binary.Write(g.eventsBufWriter, binary.LittleEndian, dBytes); err != nil {
		return err
	}

	// sync the file if the sync duration is elapsed
	if time.Since(g.prevSyncedTime) >= g.syncDuration {
		if err = g.flushAndSync(); err != nil {
			return err
		}
	}

	// if rotation events count is reached, or rotation duration is elapsed
	// rotate the events file
	g.curEventsCount++
	if g.curEventsCount >= g.rotationEventsCount || time.Since(g.fileCreationTime) >= g.rotationDuration {
		if err = g.rotateEventsFile(); err != nil {
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
	if err := g.flushAndSync(); err != nil {
		return err
	}

	if err := g.currEventsFile.Close(); err != nil {
		return err
	}

	// if no events are written to the current events file, delete the file
	// else rename the current events file so that it can be read by the compactor
	// during bootup
	if g.curEventsCount == 0 {
		// delete the current events file if no events are written
		if err := os.Remove(g.currEventsFile.Name()); err != nil {
			return err
		}
		return nil
	}

	// rename the current events file to the events file
	if err := os.Rename(filepath.Join(g.eventsPath, currentEventsFile), g.getEventsFilePath()); err != nil {
		return err
	}

	return nil
}
