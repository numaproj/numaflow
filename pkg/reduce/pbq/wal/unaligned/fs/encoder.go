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
	"log"
	"strings"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

const _IEEE = 0xedb88320

// walHeaderPreamble is the header preamble (excludes variadic key)
type walHeaderPreamble struct {
	S    int64
	E    int64
	SLen int16
}

// readMessageHeaderPreamble is the header for each unalignedWAL entry
type readMessageHeaderPreamble struct {
	EventTime  int64
	WaterMark  int64
	Offset     int64
	MessageLen int64
	KeyLen     int32
	Checksum   uint32
}

// deletionMessageHeaderPreamble is the header for each deletion event.
type deletionMessageHeaderPreamble struct {
	St       int64
	Et       int64
	SLen     int16
	KLen     int32
	Checksum uint32
}

// deletionMessage is the deletion event for a keyed window built from GC events.
type deletionMessage struct {
	St   int64
	Et   int64
	Slot string
	Key  string
}

// encoder is an encoder for the unalignedWAL entries and header.
type encoder struct{}

// newEncoder returns a new encoder
func newEncoder() *encoder {
	return &encoder{}
}

// encodeHeader encodes the header of the unalignedWAL file.
func (e *encoder) encodeHeader(id *partition.ID) ([]byte, error) {
	buf := new(bytes.Buffer)
	hp := walHeaderPreamble{
		S:    id.Start.UnixMilli(),
		E:    id.End.UnixMilli(),
		SLen: int16(len(id.Slot)),
	}

	// write the fixed values
	if err := binary.Write(buf, binary.LittleEndian, hp); err != nil {
		return nil, err
	}

	// write the slot
	if err := binary.Write(buf, binary.LittleEndian, []rune(id.Slot)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// encodeMessage encodes the given isb.ReadMessage to a binary format.
func (e *encoder) encodeMessage(message *isb.ReadMessage) ([]byte, error) {
	buf := new(bytes.Buffer)

	combinedKey := strings.Join(message.Keys, dfv1.KeysDelimitter)

	// Convert the message body to a binary format
	body, err := e.encodeWALMessageBody(message)
	if err != nil {
		return nil, err // return if there is any error
	}

	// Calculate checksum of the body
	checksum := calculateChecksum(body)

	// Prepare and encode the message header
	headerBuf, err := e.encodeWALMessageHeader(message, int64(len(body)), checksum, int32(len(combinedKey)))
	if err != nil {
		return nil, err
	}

	if err = e.writeToBuffer(buf, headerBuf.Bytes()); err != nil {
		return nil, err
	}

	// Write the combinedKey to the buffer
	if err = binary.Write(buf, binary.LittleEndian, []byte(combinedKey)); err != nil {
		return nil, err
	}

	// Write the body to the buffer
	if err = e.writeToBuffer(buf, body); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// encodeDeletionEvent encodes the given deletionMessage to a binary format.
func (e *encoder) encodeDeletionEvent(message *deletionMessage) ([]byte, error) {
	buf := new(bytes.Buffer)

	// calculate the checksum of the deletion message
	checksum := calculateChecksum([]byte(fmt.Sprintf("%d:%d:%s:%s", message.St, message.Et, message.Slot, message.Key)))

	cMessageHeader := deletionMessageHeaderPreamble{
		St:       message.St,
		Et:       message.Et,
		SLen:     int16(len(message.Slot)),
		KLen:     int32(len(message.Key)),
		Checksum: checksum,
	}

	// write the compact header
	if err := binary.Write(buf, binary.LittleEndian, cMessageHeader); err != nil {
		return nil, err
	}

	// write the slot
	if err := binary.Write(buf, binary.LittleEndian, []rune(message.Slot)); err != nil {
		return nil, err
	}

	// write the key
	if err := binary.Write(buf, binary.LittleEndian, []byte(message.Key)); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func calculateChecksum(data []byte) uint32 {
	crc32q := crc32.MakeTable(_IEEE)
	return crc32.Checksum(data, crc32q)
}

// encodeWALMessageHeader encodes the WALMessage header.
func (e *encoder) encodeWALMessageHeader(message *isb.ReadMessage, bodyLen int64, checksum uint32, keyLen int32) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)

	offset, err := message.ReadOffset.Sequence()
	if err != nil {
		log.Println("error getting sequence from offset", err.Error())
		return nil, err
	}

	// Prepare the header
	hp := &readMessageHeaderPreamble{
		Offset:     offset,
		WaterMark:  message.Watermark.UnixMilli(),
		EventTime:  message.EventTime.UnixMilli(),
		MessageLen: bodyLen,
		Checksum:   checksum,
		KeyLen:     keyLen,
	}

	// write the fixed values
	if err = binary.Write(buf, binary.LittleEndian, hp); err != nil {
		return nil, err
	}

	return buf, nil
}

// encodeWALMessageBody uses ReadMessage.Message field as the body of the unalignedWAL message, encodes the
// ReadMessage.Message, and returns.
func (e *encoder) encodeWALMessageBody(readMsg *isb.ReadMessage) ([]byte, error) {
	msgBinary, err := readMsg.Message.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("encodeWALMessageBody encountered encode err: %w", err)
	}
	return msgBinary, nil
}

func (e *encoder) writeToBuffer(buf *bytes.Buffer, data []byte) error {
	wrote, err := buf.Write(data)
	if err != nil {
		return err
	}
	expectedLength := len(data)
	if wrote != expectedLength {
		return fmt.Errorf("expected to write %d, but wrote only %d", expectedLength, wrote)
	}
	return nil
}
