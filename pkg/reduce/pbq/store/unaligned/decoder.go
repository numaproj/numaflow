package unaligned

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/partition"
)

const (
	dMessageHeaderSize = 22
	EntryHeaderSize    = 28
)

var location *time.Location
var errChecksumMismatch = fmt.Errorf("data checksum not match")

// Decoder is a decoder for the WAL entries and header.
type Decoder struct{}

// NewDecoder returns a new decoder
func NewDecoder() *Decoder {
	return &Decoder{}
}

// DecodeHeader decodes the header from the given io.Reader.
// the header of the WAL file is a partition.ID, so it returns a partition.ID.
func (d *Decoder) DecodeHeader(buf io.Reader) (*partition.ID, error) {
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

// DecodeMessage decodes the isb message from the given io.Reader.
func (d *Decoder) DecodeMessage(buf io.Reader) (*isb.ReadMessage, int64, error) {
	entryHeader, err := d.DecodeWALMessageHeader(buf)
	if err != nil {
		return nil, 0, err
	}

	entryBody, err := d.DecodeWALBody(buf, entryHeader)
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

func (d *Decoder) DecodeDeleteMessage(buf io.Reader) (*DeletionMessage, int64, error) {
	dmsg := DeletionMessage{}

	dMessageHeader := deletionMessageHeaderPreamble{}
	if err := binary.Read(buf, binary.LittleEndian, &dMessageHeader); err != nil {
		return nil, 0, err
	}

	dmsg.St = dMessageHeader.St
	dmsg.Et = dMessageHeader.Et

	// read the slot
	var slot = make([]rune, dMessageHeader.SLen)
	if err := binary.Read(buf, binary.LittleEndian, slot); err != nil {
		return nil, 0, err
	}

	dmsg.Slot = string(slot)

	// read the keys
	var key = make([]rune, dMessageHeader.KLen)
	if err := binary.Read(buf, binary.LittleEndian, key); err != nil {
		return nil, 0, err
	}

	dmsg.Key = string(key)

	size := dMessageHeaderSize + int64(dMessageHeader.SLen) + int64(dMessageHeader.KLen)
	return &dmsg, size, nil
}

func (d *Decoder) DecodeWALMessageHeader(buf io.Reader) (*readMessageHeaderPreamble, error) {
	// read the fixed vals
	var entryHeader = new(readMessageHeaderPreamble)
	err := binary.Read(buf, binary.LittleEndian, entryHeader)
	if err != nil {
		return nil, err
	}
	return entryHeader, nil
}

func (d *Decoder) DecodeWALBody(buf io.Reader, entryHeader *readMessageHeaderPreamble) (*isb.Message, error) {
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
