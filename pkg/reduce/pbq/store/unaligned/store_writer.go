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
	CurrentSegmentName = "segment-current"
)

type store struct {
	parittionID             *partition.ID // partitionID is the partition ID for the store
	currDataFp              *os.File      // currDataFp is the current data file pointer to which the data is being written
	currWriteOffset         int64         // currWriteOffset is the current write offset
	prevSyncedWOffset       int64         // prevSyncedWOffset is the previous synced write offset
	dataBufWriter           *bufio.Writer // dataBufWriter is the buffered writer for the data file
	prevSyncedTime          time.Time     // prevSyncedTime is the previous synced time
	segmentCreateTime       time.Time     // segmentCreateTime is the time when the segment is created
	encoder                 *Encoder      // encoder is the encoder for the WAL entries and header
	storeDataPath           string
	segmentSize             int64
	syncDuration            time.Duration
	openMode                int
	maxBatchSize            int64
	segmentRotationDuration time.Duration
}

// NewStore returns a new store instance
func NewWriterStore(partitionId *partition.ID, opts ...StoreWriterOption) (StoreWriter, error) {

	s := &store{
		currDataFp:        nil,
		dataBufWriter:     nil,
		currWriteOffset:   0,
		prevSyncedWOffset: 0,
		prevSyncedTime:    time.Now(),
		segmentCreateTime: time.Now(),
		encoder:           NewEncoder(),
		parittionID:       partitionId,
	}

	for _, opt := range opts {
		opt(s)
	}

	// open the current data file
	s.openFile()
	return s, nil
}

// Write writes the message to the WAL. The format as follow is
//
//	+--------------------+-------------------+-----------------+------------------+-----------------+------------+--------------+----------------+
//	| event time (int64) | watermark (int64) | offset (int64)  | msg-len (int64)  | key-len (int64) | key []byte | CRC (uint32) | message []byte |
//	+--------------------+-------------------+-----------------+------------------+-----------------+------------+--------------+----------------+
//
// CRC will be used for detecting ReadMessage corruptions.
func (s *store) Write(message *isb.ReadMessage) error {

	// encode the message
	entry, err := s.encoder.EncodeMessage(message)
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
	if s.currWriteOffset-s.prevSyncedWOffset >= s.maxBatchSize || currTime.Sub(s.prevSyncedTime) >= s.opts.syncDuration {
		if err = s.sync(); err != nil {
			return err
		}
	}

	// Only increase the write offset when we successfully write for atomicity.
	s.currWriteOffset += int64(wrote)

	// rotate the segment if the segment size is reached or segment duration is reached
	if currTime.Sub(s.segmentCreateTime) >= s.segmentRotationDuration || s.currWriteOffset >= s.opts.segmentSize {
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

	// write the wal header to the new file
	s.writeWALHeader()
	return nil
}

// rotateFile rotates the current data file to the segment file
// and updates the current data file to a new file.
func (s *store) rotateFile() error {

	// Sync data before rotating the file
	if err := s.sync(); err != nil {
		return err
	}

	// Close the current data file
	if err := s.currDataFp.Close(); err != nil {
		return err
	}

	// rename the current data file to the segment file
	if err := os.Rename(filepath.Join(s.storeDataPath, CurrentSegmentName), s.segmentFilePath(s.opts.storeDataPath)); err != nil {
		return err
	}

	// Open the next data file
	return s.openFile()
}

// segmentFilePath creates the file path for the segment file located in the storage path.
func (s *store) segmentFilePath(storePath string) string {
	return filepath.Join(storePath, SegmentPrefix+"-"+fmt.Sprintf("%d", time.Now().UnixMilli()))
}

// sync syncs the data and operation files in the store.
func (s *store) sync() error {
	// no data is written, no need to sync
	if s.currDataFp == nil {
		return nil
	}

	// flust and sync data and operation files
	if err := flushAndSync(s.dataBufWriter, s.currDataFp); err != nil {
		return err
	}

	s.prevSyncedWOffset = s.currWriteOffset
	s.prevSyncedTime = time.Now()
	return nil
}

// flushAndSync flushes the buffered data to the writer and syncs the file to disk.
func flushAndSync(w *bufio.Writer, f *os.File) error {
	if err := w.Flush(); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	return nil
}

// Close closes store
func (s *store) Close() error {
	// sync data before closing
	s.sync()

	// close the current data segment
	if err := s.currDataFp.Close(); err != nil {
		return err
	}

	return nil
}

// writeWALHeader writes the WAL header to the file.
func (w *store) writeWALHeader() (err error) {
	header, err := w.encoder.EncodeHeader(w.parittionID)
	if err != nil {
		return err
	}
	wrote, err := w.dataBufWriter.Write(header)
	if wrote != len(header) {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", len(header), wrote, err)
	}

	// Only increase the write offset when we successfully write for atomicity.
	w.currWriteOffset += int64(wrote)
	return err
}
