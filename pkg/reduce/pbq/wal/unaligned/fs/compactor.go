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
	"strconv"
	"strings"
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
	pipelineName        string
	vertexName          string
	vertexReplica       int32
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
	latestWatermark     int64
	log                 *zap.SugaredLogger
}

// fStat is to store the file name and dir path
type fStat struct {
	name    string
	dirPath string
}

func (fs *fStat) Path() string {
	return filepath.Join(fs.dirPath, fs.name)
}

// NewCompactor returns a new WAL compactor instance
func NewCompactor(ctx context.Context, pipelineName string, vertexName string, vertexReplica int32, partitionId *partition.ID, gcEventsPath string, dataSegmentWALPath string, compactedSegWALPath string, opts ...CompactorOption) (unaligned.Compactor, error) {

	c := &compactor{
		pipelineName:        pipelineName,
		vertexName:          vertexName,
		vertexReplica:       vertexReplica,
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
		latestWatermark:     -1,
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
	c.log.Infow("Compacting on bootup")
	// get all the GC events filesToReplay
	eventFiles, err := listFilesInDir(c.gcEventsWALPath, currentEventsFile, nil)
	if err != nil {
		compactorErrors.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica)), "listFiles").Inc()
		return err
	}

	// There maybe some events which were not compacted. This could happen if the compactor was stopped (restart), so we
	// should compact those before replaying persisted messages. We detect non-compacted filesToReplay by looking at unprocessed
	// gc-events in the GC WAL.
	if len(eventFiles) != 0 {
		c.log.Infow("Compacting unprocessed GC event files", zap.Int("count", len(eventFiles)))
		if err = c.compact(ctx, eventFiles); err != nil {
			compactorErrors.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica)), "compact").Inc()
			return err
		}
		// rotate the compaction file because we need to first replay the pending messages
		// if we don't rotate the file, the file name will have "current" in it and it
		// will not be considered for replay
		err = c.rotateCompactionFile()
		if err != nil {
			compactorErrors.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica)), "rotateCompactionFile").Inc()
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
		compactorErrors.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica)), "flushAndSync").Inc()
		return err
	}

	// Close the current data file
	if err = c.currCompactedFile.Close(); err != nil {
		compactorErrors.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica)), "close").Inc()
		return err
	}

	// delete the temp WIP file if it's empty
	if c.compWriteOffset == 0 {
		return os.Remove(c.currCompactedFile.Name())
	}

	// rename the current compaction file to the segment file
	if err = os.Rename(c.currCompactedFile.Name(), c.getFilePath(c.compactedSegWALPath)); err != nil {
		compactorErrors.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica)), "rename").Inc()
		return err
	}
	return err
}

// getFilePath returns the file path for new file creation
func (c *compactor) getFilePath(storePath string) string {
	return filepath.Join(storePath, compactedPrefix+"-"+fmt.Sprintf("%d-%d", time.Now().UnixNano(), c.latestWatermark))
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
				compactorErrors.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica)), "compact").Inc()
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
		// only delete the event files which has watermark less than the latest watermark of the compactor
		// this is to ensure that we don't delete the events which are not compacted
		if extractWmFromFileName(eventFile.Name()) < c.latestWatermark {
			if err = os.Remove(filepath.Join(c.gcEventsWALPath, eventFile.Name())); err != nil {
				return err
			}
			gcWALFilesCount.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica))).Dec()
		}
	}

	c.log.Infow("Compaction completed", zap.String("duration", time.Since(startTime).String()))
	compactionDuration.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica))).Observe(float64(time.Since(startTime).Milliseconds()))
	return nil
}

// buildCompactionKeyMap builds the compaction key map from the GC event filesToReplay. The map's key is the "keys" of the
// window and value is the max end-time for which the data has been forwarded to next vertex. This means we can lookup
// this map to see whether the message can be dropped and not carried to the compacted file because we have closed the
// book and have already forwarded.
func (c *compactor) buildCompactionKeyMap(eventFiles []os.FileInfo) error {
	c.compactKeyMap = make(map[string]int64)

	eventsCount := 0
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

			eventsCount++
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

	compactorEventsToCompact.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica))).Set(float64(eventsCount))
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
	filesToReplay := make([]fStat, 0)
	for _, compactedFile := range compactedFiles {
		filesToReplay = append(filesToReplay, fStat{name: compactedFile.Name(), dirPath: c.compactedSegWALPath})
	}
	for _, dataFile := range segmentFiles {
		filesToReplay = append(filesToReplay, fStat{name: dataFile.Name(), dirPath: c.dataSegmentWALPath})
	}

	if len(filesToReplay) == 0 {
		return nil
	}

	compactorFilesToCompact.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica))).Set(float64(len(filesToReplay)))

	// iterate over all the data filesToReplay and compact them
	for _, file := range filesToReplay {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.stopSignal:
			// send error, otherwise the compactor will delete
			// the event filesToReplay
			return fmt.Errorf("compactor stopped")
		default:
			c.log.Debugw("Compacting file", zap.String("file", file.Path()))
			if err = c.compactFile(file); err != nil {
				return err
			}
		}
	}

	return nil
}

// updateLatestWm updates the latest watermark value
func (c *compactor) updateLatestWm(fileName string) {
	wm := extractWmFromFileName(fileName)
	if c.latestWatermark < wm {
		c.latestWatermark = wm
	}
}

// extractWmFromFileName extracts the watermark from the file name
func extractWmFromFileName(fileName string) int64 {
	// fileName will be of format segment-<creation-time>-<watermark> or compacted-<creation-time>-<watermark> or events-<creation-time>-<watermark>
	// we need to extract the watermark from the file name and update the latest watermark value.
	var wm int64
	wm, _ = strconv.ParseInt(strings.Split(fileName, "-")[2], 10, 64)
	return wm
}

// compactFile compacts the given data file
// it copies the messages from the data file to the compaction file if the message should not be deleted
// and deletes the data file after compaction
func (c *compactor) compactFile(fs fStat) error {
	// open the data file (read only)
	dp, err := os.Open(fs.Path())
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
		key := make([]rune, mp.KeyLen)
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
	if err = os.Remove(fs.Path()); err != nil {
		return err
	}
	activeDataFilesCount.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica))).Dec()
	// update the latest watermark
	c.updateLatestWm(fs.name)
	return nil
}

// shouldKeepMessage checks if the message should be discarded or not
func (c *compactor) shouldKeepMessage(eventTime int64, key string) bool {
	// check if the key is present in the compaction key map
	ce, ok := c.compactKeyMap[key]

	// we should not discard the messages which are not older than the max end time
	if ok && eventTime >= ce {
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
	if err := binary.Write(buf, binary.LittleEndian, []rune(key)); err != nil {
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

	newFileName := c.getFilePath(c.compactedSegWALPath)
	// rename the current compaction file to the segment file
	if err := os.Rename(c.currCompactedFile.Name(), newFileName); err != nil {
		return err
	}

	compactedFileSize.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica))).Observe(float64(c.compWriteOffset))

	c.log.Debugw("Rotated compaction file", zap.String("file", newFileName))

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
	c.log.Debugw("Opening new compaction file")
	var err error

	c.currCompactedFile, err = os.OpenFile(filepath.Join(c.compactedSegWALPath, compactionInProgress), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	// reset the offset
	c.compWriteOffset = 0

	// reset the write buffer (we started off with a nil interface)
	c.compBufWriter.Reset(c.currCompactedFile)

	activeDataFilesCount.WithLabelValues(c.pipelineName, c.vertexName, strconv.Itoa(int(c.vertexReplica))).Inc()
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
