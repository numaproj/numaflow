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
	"io"
	"os"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

var location *time.Location

func init() {
	var err error
	location, err = time.LoadLocation("UTC")
	if err != nil {
		panic(fmt.Sprint("cannot load UTC", err))
	}
}

// IsCorrupted checks whether the file is corrupt
func (w *WAL) IsCorrupted() bool {
	return w.corrupted
}

func (w *WAL) readHeader() (*partition.ID, error) {
	if w.openMode == os.O_WRONLY {
		return nil, fmt.Errorf("opened using O_WRONLY")
	}

	if w.rOffset > 0 {
		return nil, fmt.Errorf("header has already been read, current readoffset is at %d", w.rOffset)
	}

	id, err := decodeHeader(w.fp)
	if err != nil {
		return nil, err
	}

	seek, err := w.fp.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	w.rOffset += seek

	return id, err
}

func (w *WAL) isEnd() bool {
	// TODO: If we are done reading, check that we got the expected
	// number of entries and return EOF.
	return w.rOffset >= w.readUpTo
}

// decodeHeader decodes the header which is encoded by encodeHeader.
func decodeHeader(buf io.Reader) (*partition.ID, error) {
	var err error
	// read the fixed vals
	var hp = new(headerPreamble)
	err = binary.Read(buf, binary.LittleEndian, hp)
	if err != nil {
		return nil, err
	}
	// read the variadic key
	var key = make([]rune, hp.KLen)
	err = binary.Read(buf, binary.LittleEndian, key)
	if err != nil {
		return nil, err
	}

	return &partition.ID{
		Start: time.UnixMilli(hp.S).In(location),
		End:   time.UnixMilli(hp.E).In(location),
		Slot:  string(key),
	}, nil
}

func (w *WAL) Read(size int64) ([]*isb.ReadMessage, bool, error) {
	if w.openMode == os.O_WRONLY {
		return nil, false, fmt.Errorf("opened using O_WRONLY")
	}

	if w.rOffset < w.wOffset {
		return nil, false, fmt.Errorf("read can only happen at startup not after any new writes")
	}

	messages := make([]*isb.ReadMessage, 0)
	// if size is greater than the number of messages in the store
	// we will assign size with the number of messages in the store
	start := w.rOffset
	for size > w.rOffset-start && !w.isEnd() {
		message, sizeRead, err := decodeEntry(w.fp)
		if err != nil {
			if err == errChecksumMismatch {
				w.corrupted = true
			}
			return nil, false, err
		}

		w.rOffset += sizeRead
		messages = append(messages, message)
	}
	currentTime := time.Now()
	if w.isEnd() {
		w.wOffset = w.rOffset
		w.prevSyncedWOffset = w.wOffset
		w.prevSyncedTime = currentTime
		w.numOfUnsyncedMsgs = 0
		return messages, true, nil
	}
	return messages, false, nil
}

// decodeEntry decodes the header which is encoded by encodeEntry.
func decodeEntry(buf io.Reader) (*isb.ReadMessage, int64, error) {
	entryHeader, err := decodeEntryHeader(buf)
	if err != nil {
		return nil, 0, err
	}

	entryBody, err := decodeEntryBody(buf, entryHeader)
	if err != nil {
		return nil, 0, err
	}
	size := EntryHeaderSize + entryHeader.MessageLen

	return &isb.ReadMessage{
		Message:    *entryBody,
		Watermark:  time.UnixMilli(entryHeader.WaterMark).In(location),
		ReadOffset: isb.SimpleIntOffset(func() int64 { return entryHeader.Offset }),
	}, size, nil
}

// decodeEntryHeader decodes the header which is encoded by encodeEntryHeader.
func decodeEntryHeader(buf io.Reader) (*entryHeaderPreamble, error) {
	// read the fixed vals
	var entryHeader = new(entryHeaderPreamble)
	err := binary.Read(buf, binary.LittleEndian, entryHeader)
	if err != nil {
		return nil, err
	}
	return entryHeader, nil
}

// decodeEntryBody decodes the header which is encoded by encodeEntryBody.
// Returns errChecksumMismatch to indicate if corrupted entry is found.
func decodeEntryBody(buf io.Reader, entryHeader *entryHeaderPreamble) (*isb.Message, error) {
	var err error

	body := make([]byte, entryHeader.MessageLen)
	size, err := buf.Read(body)
	if err != nil {
		return nil, err
	}
	if int64(size) != entryHeader.MessageLen {
		return nil, fmt.Errorf("expected to read length of %d, but wrote only %d", entryHeader.MessageLen, size)
	}

	// verify the checksum
	checksum := calculateChecksum(body)
	if checksum != entryHeader.Checksum {
		return nil, errChecksumMismatch
	}

	var message = new(isb.Message)
	dec := gob.NewDecoder(bytes.NewReader(body))
	err = dec.Decode(&message)
	if err != nil {
		return nil, err
	}
	return message, nil
}
