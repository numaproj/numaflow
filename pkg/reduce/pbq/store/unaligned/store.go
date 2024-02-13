package unaligned

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store"
)

const (
	IEEE               = 0xedb88320
	SegmentPrefix      = "segment"
	CurrentSegmentName = "current" + "-" + SegmentPrefix
)

type WAL struct {
	partitionID             *partition.ID // partitionID is the partition ID for the WAL
	currDataFp              *os.File      // currDataFp is the current data file pointer to which the data is being written
	currWriteOffset         int64         // currWriteOffset is the current write offset
	currReadOffset          int64         // currReadOffset is the current read offset
	prevSyncedWOffset       int64         // prevSyncedWOffset is the previous synced write offset
	dataBufWriter           *bufio.Writer // dataBufWriter is the buffered writer for the data file
	prevSyncedTime          time.Time     // prevSyncedTime is the previous synced time
	segmentCreateTime       time.Time     // segmentCreateTime is the time when the segment is created
	encoder                 *encoder      // encoder is the encoder for the WAL entries and header
	decoder                 *decoder      // decoder is the decoder for the WAL entries and header
	storeDataPath           string        // storeDataPath is the path to the WAL data
	segmentSize             int64         // segmentSize is the max size of the segment
	syncDuration            time.Duration // syncDuration is the duration after which the data is synced to the disk
	maxBatchSize            int64         // maxBatchSize is the maximum batch size before the data is synced to the disk
	segmentRotationDuration time.Duration // segmentRotationDuration is the duration after which the segment is rotated
	files                   []os.FileInfo
}

// NewWALWriter returns a new store writer instance
func NewWALWriter(partitionId *partition.ID, opts ...WALOption) (store.Writer, error) {

	s := &WAL{
		currDataFp:        nil,
		dataBufWriter:     nil,
		currWriteOffset:   0,
		prevSyncedWOffset: 0,
		prevSyncedTime:    time.Now(),
		segmentCreateTime: time.Now(),
		encoder:           newEncoder(),
		decoder:           newDecoder(),
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

// NewWALReader returns a new store reader instance
func NewWALReader(storeDataPath string, opts ...WALOption) (store.Reader, error) {
	var err error

	s := &WAL{
		currDataFp:        nil,
		dataBufWriter:     nil,
		currWriteOffset:   0,
		prevSyncedWOffset: 0,
		prevSyncedTime:    time.Now(),
		segmentCreateTime: time.Now(),
		encoder:           newEncoder(),
		decoder:           newDecoder(),
		storeDataPath:     storeDataPath,
	}

	for _, opt := range opts {
		opt(s)
	}

	s.files, err = filesInDir(storeDataPath)
	if err != nil {
		return nil, err
	}

	// open the current data file
	err = s.openReadFile()
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
func (s *WAL) Write(message *isb.ReadMessage) error {

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

func (s *WAL) Read(size int64) ([]*isb.ReadMessage, bool, error) {
	// Initialize variables
	var messages []*isb.ReadMessage
	var endOfFiles bool

	// Main loop to read messages
	// It will break if we have collected enough messages or if we have read through all files
	for len(messages) < int(size) && !endOfFiles {
		// If there's no current file being read
		if s.currDataFp == nil {
			// If there's no more files in the list, break
			if len(s.files) == 0 {
				endOfFiles = true
				break
			}

			// Open the next file in the list
			err := s.openReadFile()
			if err != nil {
				return nil, false, err
			}
		}

		// Try to decode a message from the file
		message, _, err := s.decoder.decodeMessage(s.currDataFp)

		// If there's an error...
		if err != nil {
			// If end of file is reached, close file and reset file pointer
			if err == io.EOF {
				err = s.currDataFp.Close()
				if err != nil {
					return nil, false, err
				}
				s.currDataFp = nil
			} else {
				// If other error, return error
				return nil, false, err
			}

			// Continue to next iteration
			continue
		}

		// If message is successfully decoded, append to the messages slice
		messages = append(messages, message)
	}

	// Return messages, the end of files flag, and no error if everything went smoothly
	return messages, endOfFiles, nil
}

func (s *WAL) openReadFile() error {
	// Open the first file in the list
	currFile, err := os.OpenFile(s.files[0].Name(), os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	s.currDataFp = currFile

	// Remove opened file from the list
	s.files = s.files[1:]

	_, err = s.decoder.decodeHeader(s.currDataFp)
	return err
}

// openFile opens a new data file
func (s *WAL) openFile() error {
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
func (s *WAL) rotateFile() error {

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
func (s *WAL) segmentFilePath(storePath string) string {
	return filepath.Join(storePath, SegmentPrefix+"-"+fmt.Sprintf("%d", time.Now().UnixMilli()))
}

// flushAndSync flushes the buffered data to the writer and syncs the file to disk.
func (s *WAL) flushAndSync() error {
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

// Close closes WAL
func (s *WAL) Close() error {
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
func (s *WAL) writeWALHeader() error {
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
