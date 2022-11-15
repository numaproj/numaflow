package wal

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
	"github.com/numaproj/numaflow/pkg/pbq/store"
)

const (
	IEEE            = 0xedb88320
	EntryHeaderSize = 28
	SegmentPrefix   = "segment"
)

// Various errors contained in DNSError.
var (
	errChecksumMismatch = errors.New("data checksum not match")
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
	// closed indicates whether the file has been closed
	closed bool
	// corrupted indicates whether the data of the file has been corrupted
	corrupted   bool
	partitionID *partition.ID
	opts        *store.StoreOptions
}

// Message footer
const (
	EOM = 1 << iota // End of Message
	EOS             // End of Stream (aka COB)
)

func NewWAL(ctx context.Context, id *partition.ID, opts *store.StoreOptions) (*WAL, error) {
	// Create wal dir if not exist
	var err error
	dir := opts.StorePath()
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.Mkdir(dir, 0644)
		if err != nil {
			return nil, err
		}
	}

	// let's open or create, initialize and return a new WAL
	wal, err := openOrCreateWAL(id, opts)

	return wal, err
}

// openOrCreateWAL open or creates a new WAL segment.
func openOrCreateWAL(id *partition.ID, opts *store.StoreOptions) (*WAL, error) {
	var err error

	filePath := getSegmentFilePath(id, opts.StorePath())
	stat, err := os.Stat(filePath)

	var fp *os.File
	var wal *WAL
	if os.IsNotExist(err) {
		// here we are explicitly giving O_WRONLY because we will not be using this to read. Our read is only during
		// boot up.
		fp, err = os.OpenFile(getSegmentFilePath(id, opts.StorePath()), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
		wal = &WAL{
			fp:          fp,
			openMode:    os.O_WRONLY,
			wOffset:     0,
			rOffset:     0,
			readUpTo:    0,
			partitionID: id,
			opts:        opts,
		}

		err = wal.writeHeader()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		// here we are explicitly giving O_RDWR because we will be using this to read too. Our read is only during
		// bootstrap.
		fp, err = os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, stat.Mode())
		if err != nil {
			return nil, err
		}
		wal = &WAL{
			fp:          fp,
			openMode:    os.O_RDWR,
			wOffset:     0,
			rOffset:     0,
			readUpTo:    stat.Size(),
			partitionID: id,
			opts:        opts,
		}
		readPartition, err := wal.readHeader()
		if err != nil {
			return nil, err
		}
		if id.Key != readPartition.Key {
			return nil, fmt.Errorf("expected partition key %s, but got %s", id.Key, readPartition.Key)
		}
	}

	return wal, err
}

// writeHeader writes the header to the file
func (w *WAL) writeHeader() error {
	header, err := encodeHeader(w.partitionID)
	if err != nil {
		return err
	}
	wrote, err := w.fp.Write(header.Bytes())
	if wrote != header.Len() {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", header.Len(), wrote, err)
	}

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
func encodeHeader(id *partition.ID) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	hp := headerPreamble{
		S:    id.Start.UnixMilli(),
		E:    id.End.UnixMilli(),
		KLen: int16(len(id.Key)),
	}

	// write the fixed values
	err := binary.Write(buf, binary.LittleEndian, hp)
	if err != nil {
		return nil, err
	}

	// write the variadic values
	err = binary.Write(buf, binary.LittleEndian, []rune(id.Key))

	return buf, err
}

func encodeEntry(message *isb.ReadMessage) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	body, err := encodeEntryBody(message)
	if err != nil {
		return nil, err
	}
	checksum := calculateChecksum(body.Bytes())

	// Writes the message header
	header, err := encodeEntryHeader(message, int64(body.Len()), checksum)
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

func encodeEntryHeader(message *isb.ReadMessage, messageLen int64, checksum uint32) (*bytes.Buffer, error) {
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
		return nil, err
	}
	return buf, nil
}

func encodeEntryBody(message *isb.ReadMessage) (*bytes.Buffer, error) {
	m := new(bytes.Buffer)
	enc := gob.NewEncoder(m)
	err := enc.Encode(message.Message)
	if err != nil {
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
func (w *WAL) Write(message *isb.ReadMessage) error {
	entry, err := encodeEntry(message)
	if err != nil {
		return err
	}

	wrote, err := w.fp.Write(entry.Bytes())
	if wrote != entry.Len() {
		return fmt.Errorf("expected to write %d, but wrote only %d, %w", entry.Len(), wrote, err)
	}
	if err != nil {
		return err
	}

	w.wOffset += int64(wrote)
	// TODO: add batch sync()
	return w.fp.Sync()
}

// Close closes the WAL Segment.
func (w *WAL) Close() error {
	err := w.fp.Sync()
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

// GC cleans up the WAL Segment.
func (w *WAL) GC() error {
	if !w.closed {
		err := w.Close()
		if err != nil {
			return err
		}
	}
	return os.Remove(w.fp.Name())
}

func getSegmentFilePath(id *partition.ID, dir string) string {
	filename := fmt.Sprintf("%s_%d.%d.%s", SegmentPrefix, id.Start.Unix(), id.End.Unix(), id.Key)
	return filepath.Join(dir, filename)
}
