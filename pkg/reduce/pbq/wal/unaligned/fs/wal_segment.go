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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal"
)

const (
	currentWALPrefix   = "current"
	IEEE               = 0xedb88320
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
}

// NewUnalignedWriteOnlyWAL returns a new store writer instance
func NewUnalignedWriteOnlyWAL(partitionId *partition.ID, opts ...WALOption) (wal.WAL, error) {

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
	}

	for _, opt := range opts {
		opt(s)
	}

	// open the current data file
	err := s.openFile()
	if err != nil {
		return nil, err
	}
	return s, nil
}

// NewUnalignedReadWriteWAL returns a new WAL instance for reading and writing
func NewUnalignedReadWriteWAL(filesToReplay []string, opts ...WALOption) (wal.WAL, error) {
	var err error

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
	}

	for _, opt := range opts {
		opt(s)
	}

	// open the current data file
	err = s.openReadFile()
	if err != nil {
		return nil, err
	}

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

	// Only increase the offset when we successfully write for atomicity.
	s.currWriteOffset += int64(wrote)

	// rotate the segment if the segment size is reached or segment duration is reached
	if currTime.Sub(s.segmentCreateTime) >= s.segmentRotationDuration || s.currWriteOffset >= s.segmentSize {
		if err = s.rotateFile(); err != nil {
			return err
		}
	}

	return nil
}

// Replay replays persisted messages during startup
// returns a channel to read messages and a channel to read errors
func (s *unalignedWAL) Replay() (<-chan *isb.ReadMessage, <-chan error) {

	// Initialize channels
	msgChan := make(chan *isb.ReadMessage)
	errChan := make(chan error)

	go func() {
		defer close(msgChan)
		defer func() { errChan = nil }()

		// Main loop to read messages
		// It will break if we have read through all filesToReplay
	replayLoop:
		for {
			// If there's no current file being read
			if s.currDataFp == nil {
				// If there's no more filesToReplay in the list, break
				if len(s.filesToReplay) == 0 {
					break replayLoop
				}

				// Open the next file in the list
				err := s.openReadFile()
				if err != nil && !errors.Is(err, io.EOF) {
					errChan <- err
					return
				}
			}

			// Try to decode a message from the file
			message, _, err := s.decoder.decodeMessage(s.currDataFp)

			// If there's an error...
			if err != nil {
				// If end of file is reached, close file and reset file pointer
				if errors.Is(err, io.EOF) {
					err = s.currDataFp.Close()
					if err != nil {
						errChan <- err
						return
					}
					s.currDataFp = nil
				} else {
					// If other error, return error
					errChan <- err
					return
				}

				// Continue to next iteration
				continue
			}

			// If message is successfully decoded, append to the messages slice
			msgChan <- message
		}

		// once replay is done, we have to open a new file to write
		// since we use the same WAL for reading and writing
		err := s.openFile()
		if err != nil {
			errChan <- err
			return
		}
	}()
	return msgChan, errChan
}

// PartitionID returns the partition ID of the store
func (s *unalignedWAL) PartitionID() *partition.ID {
	return s.partitionID
}

func (s *unalignedWAL) openReadFile() error {
	if len(s.filesToReplay) == 0 {
		return nil
	}
	// Open the first file in the list
	currFile, err := os.OpenFile(s.filesToReplay[0], os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	s.currDataFp = currFile

	// Remove opened file from the list
	s.filesToReplay = s.filesToReplay[1:]

	pid, err := s.decoder.decodeHeader(s.currDataFp)
	if err != nil {
		return err
	}
	s.partitionID = pid

	return err
}

// openFile opens a new data file
func (s *unalignedWAL) openFile() error {
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

	// rename the current data file to the segment file
	if err := os.Rename(filepath.Join(s.segmentWALPath, currentSegmentName), s.segmentFilePath(s.segmentWALPath)); err != nil {
		return err
	}

	// Open the next data file
	return s.openFile()
}

// segmentFilePath creates the file path for the segment file located in the storage path.
func (s *unalignedWAL) segmentFilePath(storePath string) string {
	return filepath.Join(storePath, segmentPrefix+"-"+fmt.Sprintf("%d", time.Now().UnixMilli()))
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
