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

package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
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

// WAL implements a write-ahead-log. It represents both reader and writer. This WAL is write heavy and read is
// infrequent, meaning a read will only happen during a boot up. WAL will only have one segment since these are
// relatively short-lived.
type WAL struct {
	// fp is the file pointer to the WAL segment
	fp *os.File
	// wOffset is the write offset as tracked by the writer
	wOffset int64
	// rOffset is the read offset as tracked when reading. Reading only
	// happens during boostrap. It is highly unlikely to reread the data
	// once boostrap sequence has been completed.
	rOffset int64
	// readUpTo is the read offset at which the reader will stop reading.
	readUpTo int64
	// openMode denotes which mode we opened the file in. Only during boot up we will open in read-write mode
	openMode int
	// createTime is the timestamp when the WAL segment is created.
	createTime time.Time
	// corrupted indicates whether the data of the file has been corrupted
	corrupted   bool
	partitionID *partition.ID
	// prevSyncedWOffset is the write offset that is already synced as tracked by the writer
	prevSyncedWOffset int64
	// prevSyncedTime is the time when the last sync was made
	prevSyncedTime    time.Time
	walStores         *walStores
	numOfUnsyncedMsgs int64
}

// writeWALHeader writes the WAL header to the file.
func (w *WAL) writeWALHeader() (err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.walStores.pipelineName,
				metrics.LabelVertex:             w.walStores.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
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

// encodeWALHeader builds the WAL header. WAL header is per WAL and has information to build the WAL partition.
// The header is of the following format.
//
//	+--------------------+------------------+------------------+-------------+
//	| start time (int64) | end time (int64) | slot-len (int16) | slot []rune |
//	+--------------------+------------------+------------------+-------------+
//
// We require the slot-len because slot is variadic.
func (w *WAL) encodeWALHeader(id *partition.ID) (buf *bytes.Buffer, err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.walStores.pipelineName,
				metrics.LabelVertex:             w.walStores.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
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

// encodeWALMessage builds the WAL message.
func (w *WAL) encodeWALMessage(message *isb.ReadMessage) (buf *bytes.Buffer, err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.walStores.pipelineName,
				metrics.LabelVertex:             w.walStores.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
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

// encodeWALMessageHeader creates the header of the WAL message. The header is mainly for checksum, verifying the
// correctness of the WALMessage.
func (w *WAL) encodeWALMessageHeader(message *isb.ReadMessage, messageLen int64, checksum uint32) (*bytes.Buffer, error) {
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
			metrics.LabelPipeline:           w.walStores.pipelineName,
			metrics.LabelVertex:             w.walStores.vertexName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
			labelErrorKind:                  "encodeWALMessageHeader",
		}).Inc()
		return nil, err
	}
	return buf, nil
}

// encodeWALMessageBody uses ReadMessage.Message field as the body of the WAL message, encodes the
// ReadMessage.Message, and returns.
func (w *WAL) encodeWALMessageBody(readMsg *isb.ReadMessage) ([]byte, error) {
	msgBinary, err := readMsg.Message.MarshalBinary()
	if err != nil {
		walErrors.With(map[string]string{
			metrics.LabelPipeline:           w.walStores.pipelineName,
			metrics.LabelVertex:             w.walStores.vertexName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
			labelErrorKind:                  "encodeWALMessageBody",
		}).Inc()
		return nil, fmt.Errorf("encodeWALMessageBody encountered encode err: %w", err)
	}
	return msgBinary, nil
}

// Write writes the message to the WAL. The format as follow is
//
//	+-------------------+----------------+-----------------+--------------+----------------+
//	| watermark (int64) | offset (int64) | msg-len (int64) | CRC (unit32) | message []byte |
//	+-------------------+----------------+-----------------+--------------+----------------+
//
// CRC will be used for detecting ReadMessage corruptions.
func (w *WAL) Write(message *isb.ReadMessage) (err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.walStores.pipelineName,
				metrics.LabelVertex:             w.walStores.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
				labelErrorKind:                  "write",
			}).Inc()
		}
	}()
	encodeStart := time.Now()
	entry, err := w.encodeWALMessage(message)
	entryEncodeLatency.With(map[string]string{
		metrics.LabelPipeline:           w.walStores.pipelineName,
		metrics.LabelVertex:             w.walStores.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
	}).Observe(float64(time.Since(encodeStart).Milliseconds()))
	if err != nil {
		return err
	}

	writeStart := time.Now()
	wrote, err := w.fp.WriteAt(entry.Bytes(), w.wOffset)
	entryWriteLatency.With(map[string]string{
		metrics.LabelPipeline:           w.walStores.pipelineName,
		metrics.LabelVertex:             w.walStores.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
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
		metrics.LabelPipeline:           w.walStores.pipelineName,
		metrics.LabelVertex:             w.walStores.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
	}).Add(float64(wrote))
	entriesCount.With(map[string]string{
		metrics.LabelPipeline:           w.walStores.pipelineName,
		metrics.LabelVertex:             w.walStores.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
	}).Inc()
	currentTime := time.Now()

	if w.wOffset-w.prevSyncedWOffset > w.walStores.maxBatchSize || currentTime.Sub(w.prevSyncedTime) > w.walStores.syncDuration {
		w.prevSyncedWOffset = w.wOffset
		w.prevSyncedTime = currentTime
		fSyncStart := time.Now()
		err = w.fp.Sync()
		fileSyncWaitTime.With(map[string]string{
			metrics.LabelPipeline:           w.walStores.pipelineName,
			metrics.LabelVertex:             w.walStores.vertexName,
			metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
		}).Observe(float64(time.Since(fSyncStart).Milliseconds()))
		w.numOfUnsyncedMsgs = 0
		return err
	}
	return err
}

// Close closes the WAL Segment.
func (w *WAL) Close() (err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metrics.LabelPipeline:           w.walStores.pipelineName,
				metrics.LabelVertex:             w.walStores.vertexName,
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
				labelErrorKind:                  "close",
			}).Inc()
		}
	}()
	start := time.Now()
	err = w.fp.Sync()
	fileSyncWaitTime.With(map[string]string{
		metrics.LabelPipeline:           w.walStores.pipelineName,
		metrics.LabelVertex:             w.walStores.vertexName,
		metrics.LabelVertexReplicaIndex: strconv.Itoa(int(w.walStores.replicaIndex)),
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
