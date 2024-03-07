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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

const (
	currentWALPrefix   = "current"
	segmentPrefix      = "segment"
	currentSegmentName = currentWALPrefix + "-" + segmentPrefix
)

// unalignedWAL is an unaligned write-ahead log
type unalignedWAL struct {
	partitionID             *partition.ID // partitionID is the partition ID for the unalignedWAL
	currDataFp              *os.File      // currDataFp is the current data file pointer to which the data is being written
	currWriteOffset         int64         // currWriteOffset is the current write offset
	prevSyncedWOffset       int64         // prevSyncedWOffset is the previous synced write offset
	dataBufWriter           *bufio.Writer // dataBufWriter is the buffered writer for the data file
	prevSyncedTime          time.Time     // prevSyncedTime is the previous synced time
	segmentCreateTime       time.Time     // segmentCreateTime is the time when the segment is created
	encoder                 *encoder      // encoder is the encoder for the unalignedWAL entries and header
	decoder                 *decoder      // decoder is the decoder for the unalignedWAL entries and header
	segmentWALPath          string        // segmentWALPath is the path to the unalignedWAL data
	compactWALPath          string        // compactWALPath is the path to the compacted unalignedWAL data
	segmentSize             int64         // segmentSize is the max size of the segment
	syncDuration            time.Duration // syncDuration is the duration after which the data is synced to the disk
	maxBatchSize            int64         // maxBatchSize is the maximum batch size before the data is synced to the disk
	segmentRotationDuration time.Duration // segmentRotationDuration is the duration after which the segment is rotated
	filesToReplay           []string
	latestWm                time.Time
	log                     *zap.SugaredLogger
}

// NewUnalignedWriteOnlyWAL returns a new store writer instance
func NewUnalignedWriteOnlyWAL(ctx context.Context, partitionId *partition.ID, opts ...WALOption) (wal.WAL, error) {

	s := &unalignedWAL{
		segmentWALPath:          dfv1.DefaultSegmentWALPath,
		segmentSize:             dfv1.DefaultWALSegmentSize,
		maxBatchSize:            dfv1.DefaultWALMaxSyncSize,
		syncDuration:            dfv1.DefaultWALSyncDuration,
		segmentRotationDuration: dfv1.DefaultWALSegmentRotationDuration,
		currDataFp:              nil,
		dataBufWriter:           nil,
		currWriteOffset:         0,
		prevSyncedWOffset:       0,
		prevSyncedTime:          time.Now(),
		segmentCreateTime:       time.Now(),
		encoder:                 newEncoder(),
		decoder:                 newDecoder(),
		partitionID:             partitionId,
		log:                     logging.FromContext(ctx),
	}

	for _, opt := range opts {
		opt(s)
	}

	// open the current data file
	err := s.openFileAndSetCurrent()
	if err != nil {
		return nil, err
	}
	return s, nil
}

// NewUnalignedReadWriteWAL returns a new WAL instance for reading and writing
func NewUnalignedReadWriteWAL(ctx context.Context, filesToReplay []string, opts ...WALOption) (wal.WAL, error) {

	s := &unalignedWAL{
		segmentWALPath:          dfv1.DefaultSegmentWALPath,
		segmentSize:             dfv1.DefaultWALSegmentSize,
		maxBatchSize:            dfv1.DefaultWALMaxSyncSize,
		syncDuration:            dfv1.DefaultWALSyncDuration,
		segmentRotationDuration: dfv1.DefaultWALSegmentRotationDuration,
		currDataFp:              nil,
		dataBufWriter:           nil,
		currWriteOffset:         0,
		prevSyncedWOffset:       0,
		prevSyncedTime:          time.Now(),
		segmentCreateTime:       time.Now(),
		encoder:                 newEncoder(),
		decoder:                 newDecoder(),
		filesToReplay:           filesToReplay,
		latestWm:                time.UnixMilli(-1),
		log:                     logging.FromContext(ctx),
	}

	for _, opt := range opts {
		opt(s)
	}

	// open a file to get the partition ID
	fp, pid, err := s.openReadFile(filesToReplay[0])
	if err != nil {
		return nil, err
	}

	// set the partition ID to the WAL
	s.partitionID = pid

	// close the file
	_ = fp.Close()

	return s, nil
}

// Write writes the message to the unalignedWAL. The format as follow is
//
//	+--------------------+-------------------+-----------------+------------------+-----------------+-------------+------------+----------------+
//	| event time (int64) | watermark (int64) | offset (int64)  | msg-len (int64)  | key-len (int64) | CRC (uint32 | key []byte | message []byte |
//	+--------------------+-------------------+-----------------+------------------+-----------------+-------------+------------+----------------+
//
// CRC will be used for detecting ReadMessage corruptions.
func (s *unalignedWAL) Write(message *isb.ReadMessage) error {

	// encode the message
	entry, err := s.encoder.encodeMessage(message)
	if err != nil {
		return err
	}

	// write the message to the data file
	wrote, err := s.dataBufWriter.Write(entry)
	if wrote != len(entry) {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", len(entry), wrote, err)
	}
	if err != nil {
		return err
	}

	currTime := time.Now()

	// sync file if the batch size is reached or sync duration is reached
	if s.currWriteOffset-s.prevSyncedWOffset >= s.maxBatchSize || currTime.Sub(s.prevSyncedTime) >= s.syncDuration {
		if err = s.flushAndSync(); err != nil {
			return err
		}
	}

	// only increase the offset when we successfully write for atomicity.
	s.currWriteOffset += int64(wrote)

	// update the watermark if its not -1
	if message.Watermark.UnixMilli() != -1 {
		s.latestWm = message.Watermark
	}

	// rotate the segment if the segment size is reached or segment duration is reached
	if currTime.Sub(s.segmentCreateTime) >= s.segmentRotationDuration || s.currWriteOffset >= s.segmentSize {
		if err = s.rotateFile(); err != nil {
			return err
		}
	}

	return nil
}

// Replay replays persisted messages during startup
// It returns a channel to read messages from replay files and a channel to read errors
func (s *unalignedWAL) Replay() (<-chan *isb.ReadMessage, <-chan error) {
	// Initialize channels
	msgChan := make(chan *isb.ReadMessage)
	errChan := make(chan error)

	go func() {
		// Clean up resources when the function returns
		defer close(msgChan)
		defer func() { errChan = nil }()

		// Iterate over all replay files
		for _, filePath := range s.filesToReplay {
			// Open the file in read mode
			fp, _, err := s.openReadFile(filePath)
			if err != nil {
				errChan <- err
				return
			}

			// Iterate over the messages in the file
			for {
				// Try to decode a message from the file
				msg, _, err := s.decoder.decodeMessage(fp)
				if err == io.EOF {
					// End of file reached, break the inner loop to proceed to the next file
					break
				} else if err != nil {
					// Error occurred, send it on error channel and return
					errChan <- err
					return
				}

				// Successful decode, send the message on the message channel
				msgChan <- msg
			}

			// Close the file
			err = fp.Close()
			if err != nil {
				errChan <- err
				return
			}
		}

		// Open a new write file once replay is done since we use the same WAL for reading and writing
		err := s.openFileAndSetCurrent()
		if err != nil {
			errChan <- err
			return
		}

		s.filesToReplay = nil
	}()

	// Return the message and error channels
	return msgChan, errChan
}

// PartitionID returns the partition ID of the store
func (s *unalignedWAL) PartitionID() *partition.ID {
	return s.partitionID
}

func (s *unalignedWAL) openReadFile(filePath string) (*os.File, *partition.ID, error) {

	// Open the first file in the list
	currFile, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, nil, err
	}

	pid, err := s.decoder.decodeHeader(currFile)
	if err != nil {
		return nil, nil, err
	}

	return currFile, pid, nil
}

// openFileAndSetCurrent opens a new data file and sets the current pointer to the opened file.
func (s *unalignedWAL) openFileAndSetCurrent() error {
	s.log.Infow("Opening new segment file")

	dataFilePath := filepath.Join(s.segmentWALPath, currentSegmentName)
	var err error
	if s.currDataFp, err = os.OpenFile(dataFilePath, os.O_WRONLY|os.O_CREATE, 0644); err != nil {
		return err
	}

	// reset the data buffer writer
	if s.dataBufWriter == nil {
		s.dataBufWriter = bufio.NewWriter(s.currDataFp)
	} else {
		s.dataBufWriter.Reset(s.currDataFp)
	}

	// reset the offset
	s.currWriteOffset = 0

	// write the WAL header to the new file
	return s.writeWALHeader()
}

// rotateFile rotates the current data file to the segment file
// and updates the current data file to a new file.
func (s *unalignedWAL) rotateFile() error {
	defer func() {
		s.segmentCreateTime = time.Now()
	}()
	// check if there are any messages written to the file
	// if not, we don't need to rotate the file
	// can happen when there are no messages to write
	// within the rotation duration
	if s.currWriteOffset == 0 {
		return nil
	}

	// Sync data before rotating the file
	if err := s.flushAndSync(); err != nil {
		return err
	}

	// Close the current data file
	if err := s.currDataFp.Close(); err != nil {
		return err
	}

	newFileName := s.segmentFilePath(s.segmentWALPath)

	// rename the current data file to the segment file
	if err := os.Rename(filepath.Join(s.segmentWALPath, currentSegmentName), newFileName); err != nil {
		return err
	}

	s.log.Debugw("Rotated segment file to - ", zap.String("fileName", newFileName))

	// Open the next data file
	return s.openFileAndSetCurrent()
}

// segmentFilePath creates the file path for the segment file located in the storage path.
func (s *unalignedWAL) segmentFilePath(storePath string) string {
	return filepath.Join(storePath, segmentPrefix+"-"+fmt.Sprintf("%d-%d", time.Now().UnixNano(), s.latestWm.UnixMilli()))
}

// flushAndSync flushes the buffered data to the writer and syncs the file to disk.
func (s *unalignedWAL) flushAndSync() error {
	if err := s.dataBufWriter.Flush(); err != nil {
		return err
	}

	if err := s.currDataFp.Sync(); err != nil {
		return err
	}

	s.prevSyncedWOffset = s.currWriteOffset
	s.prevSyncedTime = time.Now()

	return nil
}

// Close closes unalignedWAL
func (s *unalignedWAL) Close() error {
	// sync data before closing
	err := s.flushAndSync()
	if err != nil {
		return err
	}

	// close the current data segment
	if err = s.currDataFp.Close(); err != nil {
		return err
	}

	// rename the current data file to the segment file
	if err = os.Rename(filepath.Join(s.segmentWALPath, currentSegmentName), s.segmentFilePath(s.segmentWALPath)); err != nil {
		return err
	}

	return nil
}

// writeWALHeader writes the unalignedWAL header to the file.
func (s *unalignedWAL) writeWALHeader() error {
	header, err := s.encoder.encodeHeader(s.partitionID)
	if err != nil {
		return err
	}
	wrote, err := s.dataBufWriter.Write(header)
	if wrote != len(header) {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", len(header), wrote, err)
	}

	return err
}
