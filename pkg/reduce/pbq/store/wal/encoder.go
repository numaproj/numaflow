package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"strings"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

// walHeaderPreamble is the header preamble (excludes variadic key)
type walHeaderPreamble struct {
	S    int64
	E    int64
	SLen int16
}

// readMessageHeaderPreamble is the header for each WAL entry
type readMessageHeaderPreamble struct {
	WaterMark  int64
	EventTime  int64
	Offset     int64
	MessageLen int64
	KeyLen     int32
	Checksum   uint32
}

type deletionMessageHeaderPreamble struct {
	St   int64
	Et   int64
	SLen int16
	KLen int32
}

type DeletionMessage struct {
	St   int64
	Et   int64
	Slot string
	Key  string
}

// Encoder is an encoder for the WAL entries and header.
type Encoder struct {
	buf *bytes.Buffer
}

// NewEncoder returns a new encoder
func NewEncoder() *Encoder {
	return &Encoder{}
}

// EncodeHeader encodes the header of the WAL file.
func (e *Encoder) EncodeHeader(id *partition.ID) ([]byte, error) {
	e.buf.Reset()
	buf := e.buf
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
	if err := binary.Write(buf, binary.LittleEndian, []byte(id.Slot)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Encode encodes the given isb message
func (e *Encoder) EncodeMessage(message *isb.ReadMessage) ([]byte, error) {
	e.buf.Reset()

	combinedKey := strings.Join(message.Keys, dfv1.KeysDelimitter)

	// Convert the message body to a binary format
	body, err := e.encodeWALMessageBody(message)
	if err != nil {
		return nil, err // return if there is any error
	}

	// Calculate checksum of the body
	checksum := calculateChecksum(body)

	// Prepare and encode the message header
	if err := e.encodeWALMessageHeader(message, int64(len(body)), checksum, int32(len(combinedKey))); err != nil {
		return nil, err // return if there is an error
	}

	// Write the combinedKey to the buffer
	if err := e.writeToBuffer(e.buf, []byte(combinedKey)); err != nil {
		return nil, err
	}

	// Write the body to the buffer
	if err := e.writeToBuffer(e.buf, body); err != nil {
		return nil, err
	}
	return e.buf.Bytes(), nil
}

func (e *Encoder) EncodeDeletionEvent(message *DeletionMessage) ([]byte, error) {
	e.buf.Reset()

	cMessageHeader := deletionMessageHeaderPreamble{
		St:   message.St,
		Et:   message.Et,
		SLen: int16(len(message.Slot)),
		KLen: int32(len(message.Key)),
	}

	// write the compact header
	if err := binary.Write(e.buf, binary.LittleEndian, cMessageHeader); err != nil {
		return nil, err
	}

	// write the slot
	if err := binary.Write(e.buf, binary.LittleEndian, []rune(message.Slot)); err != nil {
		return nil, err
	}

	// write the keys
	if err := binary.Write(e.buf, binary.LittleEndian, []rune(message.Key)); err != nil {
		return nil, err
	}

	return e.buf.Bytes(), nil
}

func calculateChecksum(data []byte) uint32 {
	crc32q := crc32.MakeTable(IEEE)
	return crc32.Checksum(data, crc32q)
}

// encodeWALMessageHeader encodes the WALMessage header.
func (e *Encoder) encodeWALMessageHeader(message *isb.ReadMessage, bodyLen int64, checksum uint32, keyLen int32) error {

	offset, err := message.ReadOffset.Sequence()
	if err != nil {
		return err
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
	return binary.Write(e.buf, binary.LittleEndian, hp)

}

// encodeWALMessageBody uses ReadMessage.Message field as the body of the WAL message, encodes the
// ReadMessage.Message, and returns.
func (e *Encoder) encodeWALMessageBody(readMsg *isb.ReadMessage) ([]byte, error) {
	msgBinary, err := readMsg.Message.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("encodeWALMessageBody encountered encode err: %w", err)
	}
	return msgBinary, nil
}

func (e *Encoder) writeToBuffer(buf *bytes.Buffer, data []byte) error {
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
