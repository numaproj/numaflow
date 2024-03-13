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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal"
)

const (
	IEEE            = 0xedb88320
	EntryHeaderSize = 28
	SegmentPrefix   = "segment"
)

// Various errors contained in DNSError.
var (
	errChecksumMismatch = fmt.Errorf("data checksum not match")
)

// alignedWAL implements a write-ahead-log. It represents both reader and writer. This alignedWAL is write heavy and read is
// infrequent, meaning a read will only happen during a boot up. alignedWAL will only have one segment since these are
// relatively short-lived.
type alignedWAL struct {
	pipelineName      string
	vertexName        string
	replicaIndex      int32
	maxBatchSize      int64         // maxBatchSize is the maximum size of the batch before we sync the file.
	syncDuration      time.Duration // syncDuration is the duration after which the writer will sync the file.
	fp                *os.File      // fp is the file pointer to the alignedWAL segment
	wOffset           int64         // wOffset is the write offset as tracked by the writer
	rOffset           int64         // rOffset is the read offset as tracked when reading.
	readUpTo          int64         // readUpTo is the read offset at which the reader will stop reading.
	createTime        time.Time     // createTime is the timestamp when the alignedWAL segment is created.
	corrupted         bool          // corrupted indicates whether the data of the file has been corrupted
	partitionID       *partition.ID // partitionID is the partition ID of the alignedWAL segment
	prevSyncedWOffset int64         // prevSyncedWOffset is the write offset that is already synced as tracked by the writer
	prevSyncedTime    time.Time     // prevSyncedTime is the time when the last sync was made
	numOfUnsyncedMsgs int64
}

// NewAlignedWriteOnlyWAL creates a new alignedWAL instance for write-only. This will be used in happy path where we are only
// writing to the alignedWAL.
func NewAlignedWriteOnlyWAL(id *partition.ID,
	filePath string,
	maxBufferSize int64,
	syncDuration time.Duration,
	pipelineName string,
	vertexName string,
	replica int32) (wal.WAL, error) {

	w := &alignedWAL{
		pipelineName:      pipelineName,
		vertexName:        vertexName,
		replicaIndex:      replica,
		createTime:        time.Now(),
		wOffset:           0,
		rOffset:           0,
		readUpTo:          0,
		partitionID:       id,
		prevSyncedWOffset: 0,
		prevSyncedTime:    time.Time{},
		numOfUnsyncedMsgs: 0,
		maxBatchSize:      maxBufferSize,
		syncDuration:      syncDuration,
	}

	// here we are explicitly giving O_WRONLY because we will not be using this to read. Our read is only during
	// boot up.
	fp, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	w.fp = fp
	err = w.writeWALHeader()
	if err != nil {
		return nil, err
	}

	return w, nil
}

// NewAlignedReadWriteWAL creates a new alignedWAL instance for read-write. This will be used during boot up where we will be replaying
// the messages from the alignedWAL and then writing to it.
func NewAlignedReadWriteWAL(filePath string,
	maxBufferSize int64,
	syncDuration time.Duration,
	pipelineName string,
	vertexName string,
	replica int32) (wal.WAL, error) {
	w := &alignedWAL{
		pipelineName:      pipelineName,
		vertexName:        vertexName,
		replicaIndex:      replica,
		createTime:        time.Now(),
		wOffset:           0,
		rOffset:           0,
		readUpTo:          0,
		prevSyncedWOffset: 0,
		prevSyncedTime:    time.Time{},
		numOfUnsyncedMsgs: 0,
		maxBatchSize:      maxBufferSize,
		syncDuration:      syncDuration,
	}

	// here we are explicitly giving O_RDWR because we will be using this to read too. Our read is only during
	// boot up.
	fp, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	w.fp = fp

	// read the partition ID from the alignedWAL header and set it in the alignedWAL.
	readPartition, err := w.readWALHeader()
	if err != nil {
		return nil, err
	}
	w.partitionID = readPartition

	// set the read up to the end of the file.
	stat, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	w.readUpTo = stat.Size()

	return w, nil
}

// PartitionID returns the partition ID from the WAL.
func (w *alignedWAL) PartitionID() *partition.ID {
	return w.partitionID
}

// writeWALHeader writes the alignedWAL header to the file.
func (w *alignedWAL) writeWALHeader() (err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.pipelineName,
				metrics.LabelVertex:             w.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
				labelErrorKind:                  "writeWALHeader",
			}).Inc()
		}
	}()
	header, err := w.encodeWALHeader(w.partitionID)
	if err != nil {
		return err
	}
	wrote, err := w.fp.WriteAt(header.Bytes(), w.wOffset)
	if wrote != header.Len() {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", header.Len(), wrote, err)
	}

	// Only increase the write offset when we successfully write for atomicity.
	w.wOffset += int64(wrote)
	return err
}

// walHeaderPreamble is the header preamble (excludes variadic key)
type walHeaderPreamble struct {
	S    int64
	E    int64
	SLen int16
}

// readMessageHeaderPreamble is the header for each alignedWAL entry
type readMessageHeaderPreamble struct {
	WaterMark  int64
	Offset     int64
	MessageLen int64
	Checksum   uint32
}

// encodeWALHeader builds the alignedWAL header. alignedWAL header is per alignedWAL and has information to build the alignedWAL partition.
// The header is of the following format.
//
//	+--------------------+------------------+------------------+-------------+
//	| start time (int64) | end time (int64) | slot-len (int16) | slot []rune |
//	+--------------------+------------------+------------------+-------------+
//
// We require the slot-len because slot is variadic.
func (w *alignedWAL) encodeWALHeader(id *partition.ID) (buf *bytes.Buffer, err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.pipelineName,
				metrics.LabelVertex:             w.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
				labelErrorKind:                  "encodeWALHeader",
			}).Inc()
		}
	}()
	buf = new(bytes.Buffer)
	hp := walHeaderPreamble{
		S:    id.Start.UnixMilli(),
		E:    id.End.UnixMilli(),
		SLen: int16(len(id.Slot)),
	}

	// write the fixed values
	err = binary.Write(buf, binary.LittleEndian, hp)
	if err != nil {
		return nil, err
	}

	// write the variadic values
	err = binary.Write(buf, binary.LittleEndian, []rune(id.Slot))

	return buf, err
}

func calculateChecksum(data []byte) uint32 {
	crc32q := crc32.MakeTable(IEEE)
	return crc32.Checksum(data, crc32q)
}

// encodeWALMessage builds the alignedWAL message.
func (w *alignedWAL) encodeWALMessage(message *isb.ReadMessage) (buf *bytes.Buffer, err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.pipelineName,
				metrics.LabelVertex:             w.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
				labelErrorKind:                  "encodeWALMessage",
			}).Inc()
		}
	}()
	buf = new(bytes.Buffer)
	body, err := w.encodeWALMessageBody(message)
	if err != nil {
		return nil, err
	}
	checksum := calculateChecksum(body)

	// Writes the message header
	header, err := w.encodeWALMessageHeader(message, int64(len(body)), checksum)
	if err != nil {
		return nil, err
	}
	wrote, err := buf.Write(header.Bytes())
	if err != nil {
		return nil, err
	}
	if wrote != header.Len() {
		return nil, fmt.Errorf("expected to write %d, but wrote only %d, %w", header.Len(), wrote, err)
	}

	// Writes the message body
	wrote, err = buf.Write(body)
	if err != nil {
		return nil, err
	}
	if wrote != len(body) {
		return nil, fmt.Errorf("expected to write %d, but wrote only %d, %w", header.Len(), wrote, err)
	}
	return buf, nil
}

// encodeWALMessageHeader creates the header of the alignedWAL message. The header is mainly for checksum, verifying the
// correctness of the WALMessage.
func (w *alignedWAL) encodeWALMessageHeader(message *isb.ReadMessage, messageLen int64, checksum uint32) (*bytes.Buffer, error) {
	watermark := message.Watermark.UnixMilli()

	offset, err := message.ReadOffset.Sequence()
	if err != nil {
		return nil, err
	}

	msgHeader := readMessageHeaderPreamble{
		WaterMark:  watermark,
		Offset:     offset,
		MessageLen: messageLen,
		Checksum:   checksum,
	}

	// write the fixed values
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, msgHeader)
	if err != nil {
		walErrors.With(map[string]string{
			metrics.LabelPipeline:           w.pipelineName,
			metrics.LabelVertex:             w.vertexName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
			labelErrorKind:                  "encodeWALMessageHeader",
		}).Inc()
		return nil, err
	}
	return buf, nil
}

// encodeWALMessageBody uses ReadMessage.Message field as the body of the alignedWAL message, encodes the
// ReadMessage.Message, and returns.
func (w *alignedWAL) encodeWALMessageBody(readMsg *isb.ReadMessage) ([]byte, error) {
	msgBinary, err := readMsg.Message.MarshalBinary()
	if err != nil {
		walErrors.With(map[string]string{
			metrics.LabelPipeline:           w.pipelineName,
			metrics.LabelVertex:             w.vertexName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
			labelErrorKind:                  "encodeWALMessageBody",
		}).Inc()
		return nil, fmt.Errorf("encodeWALMessageBody encountered encode err: %w", err)
	}
	return msgBinary, nil
}

// Write writes the message to the alignedWAL. The format as follow is
//
//	+-------------------+----------------+-----------------+--------------+----------------+
//	| watermark (int64) | offset (int64) | msg-len (int64) | CRC (unit32) | message []byte |
//	+-------------------+----------------+-----------------+--------------+----------------+
//
// CRC will be used for detecting ReadMessage corruptions.
func (w *alignedWAL) Write(message *isb.ReadMessage) (err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.pipelineName,
				metrics.LabelVertex:             w.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
				labelErrorKind:                  "write",
			}).Inc()
		}
	}()
	encodeStart := time.Now()
	entry, err := w.encodeWALMessage(message)
	entryEncodeLatency.With(map[string]string{
		metrics.LabelPipeline:           w.pipelineName,
		metrics.LabelVertex:             w.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
	}).Observe(float64(time.Since(encodeStart).Milliseconds()))
	if err != nil {
		return err
	}

	writeStart := time.Now()
	wrote, err := w.fp.WriteAt(entry.Bytes(), w.wOffset)
	entryWriteLatency.With(map[string]string{
		metrics.LabelPipeline:           w.pipelineName,
		metrics.LabelVertex:             w.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
	}).Observe(float64(time.Since(writeStart).Milliseconds()))
	if wrote != entry.Len() {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", entry.Len(), wrote, err)
	}
	if err != nil {
		return err
	}

	w.numOfUnsyncedMsgs = w.numOfUnsyncedMsgs + 1
	// Only increase the write offset when we successfully write for atomicity.
	w.wOffset += int64(wrote)
	entriesBytesCount.With(map[string]string{
		metrics.LabelPipeline:           w.pipelineName,
		metrics.LabelVertex:             w.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
	}).Add(float64(wrote))
	entriesCount.With(map[string]string{
		metrics.LabelPipeline:           w.pipelineName,
		metrics.LabelVertex:             w.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
	}).Inc()
	currentTime := time.Now()

	if w.wOffset-w.prevSyncedWOffset > w.maxBatchSize || currentTime.Sub(w.prevSyncedTime) > w.syncDuration {
		w.prevSyncedWOffset = w.wOffset
		w.prevSyncedTime = currentTime
		fSyncStart := time.Now()
		err = w.fp.Sync()
		fileSyncWaitTime.With(map[string]string{
			metrics.LabelPipeline:           w.pipelineName,
			metrics.LabelVertex:             w.vertexName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
		}).Observe(float64(time.Since(fSyncStart).Milliseconds()))
		w.numOfUnsyncedMsgs = 0
		return err
	}
	return err
}

// Close closes the alignedWAL Segment.
func (w *alignedWAL) Close() (err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.pipelineName,
				metrics.LabelVertex:             w.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
				labelErrorKind:                  "close",
			}).Inc()
		}
	}()
	start := time.Now()
	err = w.fp.Sync()
	fileSyncWaitTime.With(map[string]string{
		metrics.LabelPipeline:           w.pipelineName,
		metrics.LabelVertex:             w.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.replicaIndex)),
	}).Observe(float64(time.Since(start).Milliseconds()))

	if err != nil {
		return err
	}

	err = w.fp.Close()
	if err != nil {
		return err
	}

	return nil
}

func getSegmentFilePath(id *partition.ID, dir string) string {
	filename := fmt.Sprintf("%s_%d.%d.%s", SegmentPrefix, id.Start.Unix(), id.End.Unix(), id.Slot)
	return filepath.Join(dir, filename)
}
