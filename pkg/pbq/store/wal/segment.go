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
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
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
	// closed indicates whether the file has been closed
	closed bool
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

// writeHeader writes the header to the file
func (w *WAL) writeHeader() (err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metricspkg.LabelPipeline: w.walStores.pipelineName,
				metricspkg.LabelVertex:   w.walStores.vertexName,
				labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
				labelErrorKind:           "writeHeader",
			}).Inc()
		}
	}()
	header, err := w.encodeHeader(w.partitionID)
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

// headerPreamble is the header preamble (excludes variadic key)
type headerPreamble struct {
	S    int64
	E    int64
	KLen int16
}

// entryHeaderPreamble is the header for each WAL entry
type entryHeaderPreamble struct {
	WaterMark  int64
	Offset     int64
	MessageLen int64
	Checksum   uint32
}

// encodeHeader builds the header. The header is of the following format.
//
//	+--------------------+------------------+-----------------+------------+
//	| start time (int64) | end time (int64) | key-len (int16) | key []rune |
//	+--------------------+------------------+-----------------+------------+
//
// We require the key-len because key is variadic.
func (w *WAL) encodeHeader(id *partition.ID) (buf *bytes.Buffer, err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metricspkg.LabelPipeline: w.walStores.pipelineName,
				metricspkg.LabelVertex:   w.walStores.vertexName,
				labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
				labelErrorKind:           "encodeHeader",
			}).Inc()
		}
	}()
	buf = new(bytes.Buffer)
	hp := headerPreamble{
		S:    id.Start.UnixMilli(),
		E:    id.End.UnixMilli(),
		KLen: int16(len(id.Key)),
	}

	// write the fixed values
	err = binary.Write(buf, binary.LittleEndian, hp)
	if err != nil {
		return nil, err
	}

	// write the variadic values
	err = binary.Write(buf, binary.LittleEndian, []rune(id.Key))

	return buf, err
}

func (w *WAL) encodeEntry(message *isb.ReadMessage) (buf *bytes.Buffer, err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metricspkg.LabelPipeline: w.walStores.pipelineName,
				metricspkg.LabelVertex:   w.walStores.vertexName,
				labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
				labelErrorKind:           "encodeEntry",
			}).Inc()
		}
	}()
	buf = new(bytes.Buffer)
	body, err := w.encodeEntryBody(message)
	if err != nil {
		return nil, err
	}
	checksum := calculateChecksum(body.Bytes())

	// Writes the message header
	header, err := w.encodeEntryHeader(message, int64(body.Len()), checksum)
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
	wrote, err = buf.Write(body.Bytes())
	if err != nil {
		return nil, err
	}
	if wrote != body.Len() {
		return nil, fmt.Errorf("expected to write %d, but wrote only %d, %w", header.Len(), wrote, err)
	}
	return buf, nil
}

func (w *WAL) encodeEntryHeader(message *isb.ReadMessage, messageLen int64, checksum uint32) (*bytes.Buffer, error) {
	watermark := message.Watermark.UnixMilli()

	offset, err := message.ReadOffset.Sequence()
	if err != nil {
		return nil, err
	}

	entryHeader := entryHeaderPreamble{
		WaterMark:  watermark,
		Offset:     offset,
		MessageLen: messageLen,
		Checksum:   checksum,
	}

	// write the fixed values
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, entryHeader)
	if err != nil {
		walErrors.With(map[string]string{
			metricspkg.LabelPipeline: w.walStores.pipelineName,
			metricspkg.LabelVertex:   w.walStores.vertexName,
			labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
			labelErrorKind:           "encodeEntryHeader",
		}).Inc()
		return nil, err
	}
	return buf, nil
}

func (w *WAL) encodeEntryBody(message *isb.ReadMessage) (*bytes.Buffer, error) {
	m := new(bytes.Buffer)
	enc := gob.NewEncoder(m)
	err := enc.Encode(message.Message)
	if err != nil {
		walErrors.With(map[string]string{
			metricspkg.LabelPipeline: w.walStores.pipelineName,
			metricspkg.LabelVertex:   w.walStores.vertexName,
			labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
			labelErrorKind:           "encodeEntryBody",
		}).Inc()
		return nil, fmt.Errorf("entry body encountered encode err: %w", err)
	}
	return m, nil
}

func calculateChecksum(data []byte) uint32 {
	crc32q := crc32.MakeTable(IEEE)
	return crc32.Checksum(data, crc32q)
}

// Write writes the message to the WAL. The format as follow is
//
//	+-------------------+----------------+-----------------+--------------+----------------+
//	| watermark (int64) | offset (int64) | msg-len (int64) | CRC (unit32) | message []byte |
//	+-------------------+----------------+-----------------+--------------+----------------+
//
// CRC will be used for detecting entry corruptions.
func (w *WAL) Write(message *isb.ReadMessage) (err error) {
	defer func() {
		if err != nil {
			walErrors.With(map[string]string{
				metricspkg.LabelPipeline: w.walStores.pipelineName,
				metricspkg.LabelVertex:   w.walStores.vertexName,
				labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
				labelErrorKind:           "write",
			}).Inc()
		}
	}()
	writeStart := time.Now()
	entry, err := w.encodeEntry(message)
	if err != nil {
		return err
	}

	wrote, err := w.fp.WriteAt(entry.Bytes(), w.wOffset)
	if wrote != entry.Len() {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", entry.Len(), wrote, err)
	}
	if err != nil {
		return err
	}

	w.numOfUnsyncedMsgs = w.numOfUnsyncedMsgs + 1
	// Only increase the write offset when we successfully write for atomicity.
	w.wOffset += int64(wrote)
	currentTime := time.Now()

	if w.wOffset-w.prevSyncedWOffset > w.walStores.maxBatchSize || currentTime.Sub(w.prevSyncedTime) > w.walStores.syncDuration {
		w.prevSyncedWOffset = w.wOffset
		w.prevSyncedTime = currentTime
		fSyncStart := time.Now()
		err = w.fp.Sync()
		fileSyncWaitTime.With(map[string]string{
			metricspkg.LabelPipeline: w.walStores.pipelineName,
			metricspkg.LabelVertex:   w.walStores.vertexName,
			labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
		}).Observe(float64(time.Since(fSyncStart).Milliseconds()))
		if err == nil {
			entryWriteTime.With(map[string]string{
				metricspkg.LabelPipeline: w.walStores.pipelineName,
				metricspkg.LabelVertex:   w.walStores.vertexName,
				labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
			}).Observe(float64(time.Since(writeStart).Milliseconds()) / float64(w.numOfUnsyncedMsgs))
			entriesCount.With(map[string]string{
				metricspkg.LabelPipeline: w.walStores.pipelineName,
				metricspkg.LabelVertex:   w.walStores.vertexName,
				labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
			}).Add(float64(w.numOfUnsyncedMsgs))
		}
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
				metricspkg.LabelPipeline: w.walStores.pipelineName,
				metricspkg.LabelVertex:   w.walStores.vertexName,
				labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
				labelErrorKind:           "close",
			}).Inc()
		}
	}()
	start := time.Now()
	err = w.fp.Sync()
	fileSyncWaitTime.With(map[string]string{
		metricspkg.LabelPipeline: w.walStores.pipelineName,
		metricspkg.LabelVertex:   w.walStores.vertexName,
		labelVertexReplicaIndex:  strconv.Itoa(int(w.walStores.replicaIndex)),
	}).Observe(float64(time.Since(start).Milliseconds()))

	if err != nil {
		return err
	}

	err = w.fp.Close()
	if err != nil {
		return err
	}

	w.closed = true

	return nil
}

func getSegmentFilePath(id *partition.ID, dir string) string {
	filename := fmt.Sprintf("%s_%d.%d.%s", SegmentPrefix, id.Start.Unix(), id.End.Unix(), id.Key)
	return filepath.Join(dir, filename)
}
