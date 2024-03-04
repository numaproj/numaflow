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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal/unaligned"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

const (
	compactedPrefix      = "compacted"
	compactionInProgress = "current" + "-" + compactedPrefix
)

// compactor is a compactor for the data filesToReplay
type compactor struct {
	partitionID         *partition.ID
	compactKeyMap       map[string]int64
	gcEventsWALPath     string
	dataSegmentWALPath  string
	compactedSegWALPath string
	currCompactedFile   *os.File
	compWriteOffset     int64
	compBufWriter       *bufio.Writer
	prevSyncedTime      time.Time
	maxFileSize         int64
	mu                  sync.Mutex
	dc                  *decoder
	ec                  *encoder
	compactionDuration  time.Duration
	syncDuration        time.Duration
	stopSignal          chan struct{}
	doneCh              chan struct{}
	log                 *zap.SugaredLogger
}

// NewCompactor returns a new WAL compactor instance
func NewCompactor(ctx context.Context, partitionId *partition.ID, gcEventsPath string, dataSegmentWALPath string, compactedSegWALPath string, opts ...CompactorOption) (unaligned.Compactor, error) {

	c := &compactor{
		compactionDuration:  dfv1.DefaultWALCompactionDuration,
		maxFileSize:         dfv1.DefaultWALCompactorMaxFileSize,
		syncDuration:        dfv1.DefaultWALCompactorSyncDuration, // FIXME(WAL): we need to sync only at the end
		partitionID:         partitionId,
		dataSegmentWALPath:  dataSegmentWALPath,
		compactedSegWALPath: compactedSegWALPath,
		gcEventsWALPath:     gcEventsPath,
		compactKeyMap:       make(map[string]int64),
		prevSyncedTime:      time.Now(),
		mu:                  sync.Mutex{},
		dc:                  newDecoder(),
		ec:                  newEncoder(),
		compWriteOffset:     0,
		compBufWriter:       bufio.NewWriter(nil),
		doneCh:              make(chan struct{}),
		stopSignal:          make(chan struct{}),
		log:                 logging.FromContext(ctx),
	}

	for _, opt := range opts {
		opt(c)
	}

	// Create WAL dir if not exist
	var err error
	if _, err = os.Stat(c.dataSegmentWALPath); os.IsNotExist(err) {
		err = os.Mkdir(c.dataSegmentWALPath, 0755)
		if err != nil {
			return nil, err
		}
	}

	// Create event dir if not exist
	if _, err = os.Stat(c.gcEventsWALPath); os.IsNotExist(err) {
		err = os.Mkdir(c.gcEventsWALPath, 0755)
		if err != nil {
			return nil, err
		}
	}

	// Create compacted dir if not exist
	if _, err = os.Stat(c.compactedSegWALPath); os.IsNotExist(err) {
		err = os.Mkdir(c.compactedSegWALPath, 0755)
		if err != nil {
			return nil, err
		}
	}

	// if the file with the compactionInProgress name exists, it means the compactor was stopped
	// abruptly we should rename the file, so that it gets considered for replay
	currCompactionFileName := filepath.Join(c.compactedSegWALPath, compactionInProgress)
	if _, err = os.Stat(currCompactionFileName); err != nil && !os.IsNotExist(err) {
		if err = os.Rename(currCompactionFileName, c.getFilePath(c.compactedSegWALPath)); err != nil {
			return nil, err
		}
	}

	// open the first compaction file to write to
	if err = c.openCompactionFile(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *compactor) compactOnBootup(ctx context.Context) error {
	// get all the GC events filesToReplay
	eventFiles, err := listFilesInDir(c.gcEventsWALPath, currentWALPrefix, nil)
	if err != nil {
		return err
	}

	// There maybe some events which were not compacted. This could happen if the compactor was stopped (restart), so we
	// should compact those before replaying persisted messages. We detect non-compacted filesToReplay by looking at unprocessed
	// gc-events in the GC WAL.
	if len(eventFiles) != 0 {
		if err = c.compact(ctx, eventFiles); err != nil {
			return err
		}
		// rotate the compaction file because we need to first replay the pending messages
		// if we don't rotate the file, the file name will have "current" in it and it
		// will not be considered for replay
		err = c.rotateCompactionFile()
		if err != nil {
			return err
		}
	}

	return nil
}

// Start starts the compactor.
func (c *compactor) Start(ctx context.Context) error {
	// in case of incomplete compaction we should compact the data filesToReplay
	// before starting the compactor
	if err := c.compactOnBootup(ctx); err != nil {
		return err
	}

	// NOTE: this go routine will run while replay is running, but shouldn't have any side-effects.
	go c.keepCompacting(ctx)

	return nil
}

// Stop stops the compactor.
func (c *compactor) Stop() error {
	var err error

	// send the stop signal
	close(c.stopSignal)
	c.log.Info("Sent 'close' signal to stop the compactor, waiting for it to stop the 'done' channel")
	// wait for the compactor to stop
	<-c.doneCh

	// flush the buffer and sync the file
	if err = c.flushAndSync(); err != nil {
		return err
	}

	// Close the current data file
	if err = c.currCompactedFile.Close(); err != nil {
		return err
	}

	// delete the temp WIP file if it's empty
	if c.compWriteOffset == 0 {
		return os.Remove(c.currCompactedFile.Name())
	}

	// rename the current compaction file to the segment file
	if err = os.Rename(c.currCompactedFile.Name(), c.getFilePath(c.compactedSegWALPath)); err != nil {
		return err
	}

	return err
}

// getFilePath returns the file path for new file creation
func (c *compactor) getFilePath(storePath string) string {
	return filepath.Join(storePath, compactedPrefix+"-"+fmt.Sprintf("%d", time.Now().UnixNano()))
}

// keepCompacting keeps compacting the data filesToReplay every compaction duration
func (c *compactor) keepCompacting(ctx context.Context) {
	compTimer := time.NewTicker(c.compactionDuration)
	for {
		select {
		case <-ctx.Done():
			close(c.doneCh)
			return
		case <-c.stopSignal:
			close(c.doneCh)
			return
		case <-compTimer.C:
			// get all the events filesToReplay
			eventFiles, _ := listFilesInDir(c.gcEventsWALPath, currentEventsFile, nil)
			if len(eventFiles) >= 0 {
				err := c.compact(ctx, eventFiles)
				// TODO: retry, if its not ctx or stop signal error
				if err != nil {
					c.log.Errorw("Error while compacting", zap.Error(err))
				}
			}
		}
	}

}

// compact reads all the events file and constructs the compaction key map
// and then compacts the data filesToReplay based on the compaction key map
func (c *compactor) compact(ctx context.Context, eventFiles []os.FileInfo) error {
	startTime := time.Now()
	// build the compaction key map
	err := c.buildCompactionKeyMap(eventFiles)
	if err != nil {
		return err
	}

	// compact the data filesToReplay based on the compaction key map
	err = c.compactDataFiles(ctx)
	if err != nil {
		return err
	}

	// delete the events filesToReplay
	for _, eventFile := range eventFiles {
		if err = os.Remove(filepath.Join(c.gcEventsWALPath, eventFile.Name())); err != nil {
			return err
		}
	}

	c.log.Debugw("compaction completed", zap.Duration("duration", time.Since(startTime)))
	return nil
}

// buildCompactionKeyMap builds the compaction key map from the GC event filesToReplay. The map's key is the "keys" of the
// window and value is the max end-time for which the data has been forwarded to next vertex. This means we can lookup
// this map to see whether the message can be dropped and not carried to the compacted file because we have closed the
// book and have already forwarded.
func (c *compactor) buildCompactionKeyMap(eventFiles []os.FileInfo) error {
	c.compactKeyMap = make(map[string]int64)

	for _, eventFile := range eventFiles {
		// read the GC events file
		opFile, err := os.Open(filepath.Join(c.gcEventsWALPath, eventFile.Name()))
		if err != nil {
			return err
		}

		// iterate over all the GC events
		for {
			var cEvent *deletionMessage
			cEvent, _, err = c.dc.decodeDeletionMessage(opFile)
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				// close the file before returning
				_ = opFile.Close()
				// return the previous main error, not the close error
				return err
			}

			// track the max end time for each key
			if _, ok := c.compactKeyMap[cEvent.Key]; !ok {
				c.compactKeyMap[cEvent.Key] = cEvent.Et
			} else {
				if c.compactKeyMap[cEvent.Key] < cEvent.Et {
					c.compactKeyMap[cEvent.Key] = cEvent.Et
				}
			}
		}

		_ = opFile.Close()
	}

	return nil
}

// compactDataFiles compacts the data filesToReplay
func (c *compactor) compactDataFiles(ctx context.Context) error {

	// if there are no events to compact, return
	// during replay this list should be == 0, hence it was okay to start the go routine early on
	if len(c.compactKeyMap) == 0 {
		return nil
	}

	// get all the compacted files
	compactedFiles, err := listFilesInDir(c.compactedSegWALPath, currentWALPrefix, sortFunc)
	if err != nil {
		return err
	}

	// get all the segment files
	segmentFiles, err := listFilesInDir(c.dataSegmentWALPath, currentWALPrefix, sortFunc)
	if err != nil {
		return err
	}

	// we should consider the compacted files first, since the compacted files will have the oldest data
	filesToReplay := make([]string, 0)
	for _, compactedFile := range compactedFiles {
		filesToReplay = append(filesToReplay, filepath.Join(c.compactedSegWALPath, compactedFile.Name()))
	}
	for _, dataFile := range segmentFiles {
		filesToReplay = append(filesToReplay, filepath.Join(c.dataSegmentWALPath, dataFile.Name()))
	}

	if len(filesToReplay) == 0 {
		return nil
	}

	// iterate over all the data filesToReplay and compact them
	for _, filePath := range filesToReplay {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopSignal:
			// send error, otherwise the compactor will delete
			// the event filesToReplay
			return fmt.Errorf("compactor stopped")
		default:
			c.log.Debugw("compacting file", zap.String("file", filePath))
			if err = c.compactFile(filePath); err != nil {
				return err
			}
		}
	}

	return nil
}

// compactFile compacts the given data file
// it copies the messages from the data file to the compaction file if the message should not be deleted
// and deletes the data file after compaction
func (c *compactor) compactFile(fp string) error {
	// open the data file (read only)
	dp, err := os.Open(fp)
	if err != nil {
		return err
	}

	// close the file before returning
	defer func(dp *os.File) {
		_ = dp.Close()
	}(dp)

	// read and decode the unalignedWAL header
	_, err = c.dc.decodeHeader(dp)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err

	}

readLoop:
	for {
		// read and decode the unalignedWAL message header
		mp, err := c.dc.decodeWALMessageHeader(dp)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break readLoop // end of file reached, break the loop
			}
			return err
		}

		// read the key
		key := make([]byte, mp.KeyLen)
		err = binary.Read(dp, binary.LittleEndian, &key)
		if err != nil {
			return err
		}

		// read the payload
		var payload = make([]byte, mp.MessageLen)
		read, err := dp.Read(payload)
		if err != nil {
			return err
		}

		mp.Checksum = calculateChecksum(payload)

		if read != int(mp.MessageLen) {
			return fmt.Errorf("expected to read length of %d, but read only %d", mp.MessageLen, read)
		}

		// skip deleted messages
		// we should copy the message only if the message should not be deleted
		if !c.shouldKeepMessage(mp.EventTime, string(key)) {
			continue
		}

		// write the message to the output file
		if err = c.writeToFile(mp, string(key), payload); err != nil {
			return err
		}

	}

	// delete the file, since it's been compacted
	if err = os.Remove(fp); err != nil {
		return err
	}
	return nil
}

// shouldKeepMessage checks if the message should be discarded or not
func (c *compactor) shouldKeepMessage(eventTime int64, key string) bool {
	// check if the key is present in the compaction key map
	ce, ok := c.compactKeyMap[key]

	// we should only delete the messages which are older than the max end time
	if ok && ce < eventTime {
		return true
	}
	return false
}

// writeToFile writes the message to the compacted file and rotates the file if the max file size is reached
func (c *compactor) writeToFile(header *readMessageHeaderPreamble, key string, payload []byte) error {
	buf := new(bytes.Buffer)
	// write the header
	if err := binary.Write(buf, binary.LittleEndian, header); err != nil {
		return err
	}

	// write the key
	if err := binary.Write(buf, binary.LittleEndian, []byte(key)); err != nil {
		return err
	}

	// write the payload
	if wrote, err := buf.Write(payload); err != nil {
		return err
	} else if wrote != len(payload) {
		return fmt.Errorf("expected to write %d, but wrote only %d", len(payload), wrote)
	}

	bytesCount, err := c.compBufWriter.Write(buf.Bytes())
	if err != nil {
		return err
	}

	// update the write offset
	c.compWriteOffset += int64(bytesCount)

	// sync the file if the sync duration is reached
	if time.Since(c.prevSyncedTime) > c.syncDuration {
		if err = c.flushAndSync(); err != nil {
			return err
		}
	}

	if c.compWriteOffset >= c.maxFileSize {
		if err = c.rotateCompactionFile(); err != nil {
			return err
		}
	}

	return nil
}

// rotateCompactionFile rotates the compaction file
func (c *compactor) rotateCompactionFile() error {

	// flush the buffer and sync the file
	if err := c.flushAndSync(); err != nil {
		return err
	}

	// Close the current data file
	if err := c.currCompactedFile.Close(); err != nil {
		return err
	}

	// rename the current compaction file to the segment file
	if err := os.Rename(c.currCompactedFile.Name(), c.getFilePath(c.compactedSegWALPath)); err != nil {
		return err
	}

	// Open the next data file
	return c.openCompactionFile()
}

// flushAndSync flushes the buffer and calls fs.sync on the compaction file
func (c *compactor) flushAndSync() error {
	if err := c.compBufWriter.Flush(); err != nil {
		return err
	}
	if err := c.currCompactedFile.Sync(); err != nil {
		return err
	}

	// update the previous synced time
	c.prevSyncedTime = time.Now()
	return nil
}

// openCompactionFile opens a new compaction file
func (c *compactor) openCompactionFile() error {
	var err error

	c.currCompactedFile, err = os.OpenFile(filepath.Join(c.compactedSegWALPath, compactionInProgress), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	// reset the offset
	c.compWriteOffset = 0

	// reset the write buffer (we started off with a nil interface)
	c.compBufWriter.Reset(c.currCompactedFile)

	return c.writeWALHeader()
}

// writeWALHeader writes the unalignedWAL header to the file.
func (c *compactor) writeWALHeader() error {
	header, err := c.ec.encodeHeader(c.partitionID)
	if err != nil {
		return err
	}
	wrote, err := c.compBufWriter.Write(header)
	if wrote != len(header) {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", len(header), wrote, err)
	}

	return err
}
