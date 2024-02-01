package unaligned

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

const (
	IEEE               = 0xedb88320
	SegmentPrefix      = "segment"
	CurrentSegmentName = "current" + "-" + SegmentPrefix
)

type store struct {
	partitionID             *partition.ID // partitionID is the partition ID for the store
	currDataFp              *os.File      // currDataFp is the current data file pointer to which the data is being written
	currWriteOffset         int64         // currWriteOffset is the current write offset
	prevSyncedWOffset       int64         // prevSyncedWOffset is the previous synced write offset
	dataBufWriter           *bufio.Writer // dataBufWriter is the buffered writer for the data file
	prevSyncedTime          time.Time     // prevSyncedTime is the previous synced time
	segmentCreateTime       time.Time     // segmentCreateTime is the time when the segment is created
	encoder                 *encoder      // encoder is the encoder for the WAL entries and header
	storeDataPath           string
	segmentSize             int64
	syncDuration            time.Duration
	maxBatchSize            int64
	segmentRotationDuration time.Duration
}

// NewWriterStore returns a new store instance
func NewWriterStore(partitionId *partition.ID, opts ...StoreWriterOption) (StoreWriter, error) {

	s := &store{
		currDataFp:        nil,
		dataBufWriter:     nil,
		currWriteOffset:   0,
		prevSyncedWOffset: 0,
		prevSyncedTime:    time.Now(),
		segmentCreateTime: time.Now(),
		encoder:           newEncoder(),
		partitionID:       partitionId,
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

// Write writes the message to the WAL. The format as follow is
//
//	+--------------------+-------------------+-----------------+------------------+-----------------+-------------+------------+----------------+
//	| event time (int64) | watermark (int64) | offset (int64)  | msg-len (int64)  | key-len (int64) | CRC (uint32 | key []byte | message []byte |
//	+--------------------+-------------------+-----------------+------------------+-----------------+-------------+------------+----------------+
//
// CRC will be used for detecting ReadMessage corruptions.
func (s *store) Write(message *isb.ReadMessage) error {

	// encode the message
	entry, err := s.encoder.encodeMessage(message)
	if err != nil {
		println("Error while encoding the message", err.Error())
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

// openFile opens a new data file
func (s *store) openFile() error {
	dataFilePath := filepath.Join(s.storeDataPath, CurrentSegmentName)

	var err error
	if s.currDataFp, err = os.OpenFile(dataFilePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644); err != nil {
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

	// write the wal header to the new file
	return s.writeWALHeader()
}

// rotateFile rotates the current data file to the segment file
// and updates the current data file to a new file.
func (s *store) rotateFile() error {

	// Sync data before rotating the file
	if err := s.flushAndSync(); err != nil {
		return err
	}

	// Close the current data file
	if err := s.currDataFp.Close(); err != nil {
		return err
	}

	// rename the current data file to the segment file
	if err := os.Rename(filepath.Join(s.storeDataPath, CurrentSegmentName), s.segmentFilePath(s.storeDataPath)); err != nil {
		return err
	}

	// Open the next data file
	return s.openFile()
}

// segmentFilePath creates the file path for the segment file located in the storage path.
func (s *store) segmentFilePath(storePath string) string {
	return filepath.Join(storePath, SegmentPrefix+"-"+fmt.Sprintf("%d", time.Now().UnixMilli()))
}

// flushAndSync flushes the buffered data to the writer and syncs the file to disk.
func (s *store) flushAndSync() error {
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

// Close closes store
func (s *store) Close() error {
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
	if err = os.Rename(filepath.Join(s.storeDataPath, CurrentSegmentName), s.segmentFilePath(s.storeDataPath)); err != nil {
		return err
	}

	return nil
}

// writeWALHeader writes the WAL header to the file.
func (s *store) writeWALHeader() error {
	header, err := s.encoder.encodeHeader(s.partitionID)
	if err != nil {
		return err
	}
	wrote, err := s.dataBufWriter.Write(header)
	if wrote != len(header) {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", len(header), wrote, err)
	}

	// Only increase the offset when we successfully write for atomicity.
	s.currWriteOffset += int64(wrote)
	return err
}
