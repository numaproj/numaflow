// Package ot represents the offset-timeline pair and its corresponding encoder and decoder.
package ot

import (
	"bytes"
	"encoding/binary"
)

// Value is used in the JetStream offset timeline bucket as the value for the given processor entity key.
type Value struct {
	Offset    int64
	Watermark int64
}

// EncodeToBytes encodes a Value object into byte array.
func (v Value) EncodeToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeToOTValue decodes the given byte array into a Value object.
func DecodeToOTValue(b []byte) (Value, error) {
	var v Value
	buf := bytes.NewReader(b)
	err := binary.Read(buf, binary.LittleEndian, &v)
	if err != nil {
		return Value{}, err
	}
	return v, nil
}
