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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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
func (w *alignedWAL) IsCorrupted() bool {
	return w.corrupted
}

func (w *alignedWAL) readWALHeader() (*partition.ID, error) {
	if w.rOffset > 0 {
		return nil, fmt.Errorf("header has already been read, current readoffset is at %d", w.rOffset)
	}

	id, err := decodeWALHeader(w.fp)
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

func (w *alignedWAL) isEnd() bool {
	// TODO: If we are done reading, check that we got the expected
	// number of entries and return EOF.
	return w.rOffset >= w.readUpTo
}

// decodeWALHeader decodes the header which is encoded by encodeWALHeader.
func decodeWALHeader(buf io.Reader) (*partition.ID, error) {
	var err error
	// read the fixed values
	var hp = new(walHeaderPreamble)
	err = binary.Read(buf, binary.LittleEndian, hp)
	if err != nil {
		return nil, err
	}
	// read the variadic slot
	var slot = make([]rune, hp.SLen)
	err = binary.Read(buf, binary.LittleEndian, slot)
	if err != nil {
		return nil, err
	}

	return &partition.ID{
		Start: time.UnixMilli(hp.S).In(location),
		End:   time.UnixMilli(hp.E).In(location),
		Slot:  string(slot),
	}, nil
}

// Replay replays the alignedWAL messages, returns a channel to read messages and a channel to read errors.
// channel will be closed after all the messages are read from the alignedWAL.
func (w *alignedWAL) Replay() (<-chan *isb.ReadMessage, <-chan error) {
	messages := make(chan *isb.ReadMessage)
	errs := make(chan error)

	go func() {
		defer close(messages)
		defer func() { errs = nil }()

		// decode read message and send it to the channel
		// dont use Read method
		for !w.isEnd() {
			message, sizeRead, err := decodeReadMessage(w.fp)
			if err != nil {
				if errors.Is(err, errChecksumMismatch) {
					w.corrupted = true
				}
				errs <- err
				return
			}

			w.rOffset += sizeRead
			messages <- message
		}
		w.wOffset = w.rOffset
		w.prevSyncedWOffset = w.wOffset
		w.prevSyncedTime = time.Now()
		w.numOfUnsyncedMsgs = 0
	}()
	return messages, errs
}

// decodeReadMessage decodes the WALMessage which is encoded by encodeWALMessage.
func decodeReadMessage(buf io.Reader) (*isb.ReadMessage, int64, error) {
	entryHeader, err := decodeWALMessageHeader(buf)
	if err != nil {
		return nil, 0, err
	}

	entryBody, err := decodeWALBody(buf, entryHeader)
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

// decodeWALMessageHeader decodes the WALMessage header which is encoded by encodeWALMessageHeader.
func decodeWALMessageHeader(buf io.Reader) (*readMessageHeaderPreamble, error) {
	// read the fixed vals
	var entryHeader = new(readMessageHeaderPreamble)
	err := binary.Read(buf, binary.LittleEndian, entryHeader)
	if err != nil {
		return nil, err
	}
	return entryHeader, nil
}

// decodeWALBody decodes the WALMessage body which is encoded by encodeWALMessageBody.
// Returns errChecksumMismatch to indicate if corrupted entry is found.
func decodeWALBody(buf io.Reader, entryHeader *readMessageHeaderPreamble) (*isb.Message, error) {
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
	err = message.UnmarshalBinary(body)
	if err != nil {
		return nil, err
	}
	return message, nil
}
