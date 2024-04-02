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

package fs

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal/unaligned"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/window"
)

const (
	eventsFilePrefix  = "events"
	currentEventsFile = "current" + "-" + eventsFilePrefix
)

type gcEventsWAL struct {
	pipelineName        string        // pipeline name
	vertexName          string        // vertex name
	replicaIndex        int32         // replica index
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
	latestEndTime       time.Time     // latest end time of the window
	log                 *zap.SugaredLogger
}

// NewGCEventsWAL returns a new GCEventsWAL
func NewGCEventsWAL(ctx context.Context, pipelineName string, vertexName string, replicaIndex int32, opts ...GCEventsWALOption) (unaligned.GCEventsWAL, error) {
	gw := &gcEventsWAL{
		pipelineName:        pipelineName,
		vertexName:          vertexName,
		replicaIndex:        replicaIndex,
		syncDuration:        dfv1.DefaultGCEventsWALSyncDuration,
		rotationDuration:    dfv1.DefaultGCEventsWALRotationDuration,
		eventsPath:          dfv1.DefaultGCEventsWALEventsPath,
		currEventsFile:      nil,
		eventsBufWriter:     nil,
		prevSyncedTime:      time.Now(),
		encoder:             newEncoder(),
		rotationEventsCount: dfv1.DefaultGCEventsWALRotationEventsCount,
		curEventsCount:      0,
		fileCreationTime:    time.Now(),
		latestEndTime:       time.UnixMilli(-1),
		log:                 logging.FromContext(ctx),
	}

	for _, opt := range opts {
		opt(gw)
	}

	var err error
	// Create event dir if not exist
	if _, err = os.Stat(gw.eventsPath); os.IsNotExist(err) {
		err = os.Mkdir(gw.eventsPath, 0755)
		if err != nil {
			gcWALErrors.WithLabelValues(gw.pipelineName, gw.vertexName, strconv.Itoa(int(gw.replicaIndex)), "createDir").Inc()
			return nil, err
		}
	}

	// open the events file
	err = gw.openEventsFile()

	return gw, err
}

// rotateEventsFile rotates the events file and updates the current events file
// with the new file
func (g *gcEventsWAL) rotateEventsFile() error {
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

	newFilePath := g.getEventsFilePath()
	// rename the current event file to the new file path
	if err = os.Rename(filepath.Join(g.eventsPath, currentEventsFile), newFilePath); err != nil {
		return err
	}
	gcWALFileEventsCount.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex))).Observe(float64(g.curEventsCount))
	g.log.Debugw("Rotated the gc events segment", zap.String("new-events-file", newFilePath))
	return g.openEventsFile()
}

// getEventsFilePath returns the events file path
func (g *gcEventsWAL) getEventsFilePath() string {
	return filepath.Join(g.eventsPath, eventsFilePrefix+"-"+fmt.Sprintf("%d-%d", time.Now().UnixNano(), g.latestEndTime.UnixMilli()))
}

// openEventsFile opens a new events file to write to
func (g *gcEventsWAL) openEventsFile() error {
	g.log.Debugw("Opening a new gc events segment")
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

	gcWALFilesCount.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex))).Inc()
	return nil
}

// PersistGCEvent persists the GC event of the window
func (g *gcEventsWAL) PersistGCEvent(window window.TimedWindow) error {

	if g.currEventsFile == nil {
		gcWALErrors.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex)), "fileNotOpen").Inc()
		return fmt.Errorf("events file is not open")
	}

	dms := &deletionMessage{
		St:   window.StartTime().UnixMilli(),
		Et:   window.EndTime().UnixMilli(),
		Slot: window.Slot(),
		Key:  strings.Join(window.Keys(), dfv1.KeysDelimitter),
	}

	// encode and write the deletion message
	dBytes, err := g.encoder.encodeDeletionMessage(dms)
	if err != nil {
		gcWALErrors.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex)), "encode").Inc()
		return err
	}

	if err = binary.Write(g.eventsBufWriter, binary.LittleEndian, dBytes); err != nil {
		gcWALErrors.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex)), "write").Inc()
		return err
	}

	// sync the file if the sync duration is elapsed
	if time.Since(g.prevSyncedTime) >= g.syncDuration {
		if err = g.flushAndSync(); err != nil {
			gcWALErrors.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex)), "sync").Inc()
			return err
		}
	}

	// update the latest end time of the window
	if window.EndTime().After(g.latestEndTime) {
		g.latestEndTime = window.EndTime()
	}

	// if rotation events count is reached, or rotation duration is elapsed
	// rotate the events file
	g.curEventsCount++
	if g.curEventsCount >= g.rotationEventsCount || time.Since(g.fileCreationTime) >= g.rotationDuration {
		if err = g.rotateEventsFile(); err != nil {
			gcWALErrors.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex)), "rotate").Inc()
			return err
		}
	}

	gcWALEntriesCount.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex))).Inc()
	gcWALBytesCount.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex))).Add(float64(len(dBytes)))

	return nil
}

func (g *gcEventsWAL) flushAndSync() error {
	if err := g.eventsBufWriter.Flush(); err != nil {
		return err
	}

	g.prevSyncedTime = time.Now()
	return g.currEventsFile.Sync()
}

// Close closes the GCEventsWAL by flushing and syncing the current events file
func (g *gcEventsWAL) Close() error {
	g.log.Info("Closing the GC events WAL")

	if err := g.flushAndSync(); err != nil {
		gcWALErrors.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex)), "flush").Inc()
		return err
	}

	if err := g.currEventsFile.Close(); err != nil {
		gcWALErrors.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex)), "close").Inc()
		return err
	}

	// if no events are written to the current events file, delete the file
	// else rename the current events file so that it can be read by the compactor
	// during startup
	if g.curEventsCount == 0 {
		// delete the current events file if no events are written
		if err := os.Remove(g.currEventsFile.Name()); err != nil {
			gcWALErrors.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex)), "delete").Inc()
			return err
		}
		gcWALFilesCount.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex))).Dec()
		return nil
	}

	// rename the current events file to the events file
	if err := os.Rename(filepath.Join(g.eventsPath, currentEventsFile), g.getEventsFilePath()); err != nil {
		gcWALErrors.WithLabelValues(g.pipelineName, g.vertexName, strconv.Itoa(int(g.replicaIndex)), "rename").Inc()
		return err
	}

	return nil
}
