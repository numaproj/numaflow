package otbucket

import (
	"bytes"
	"encoding/binary"
)

// OTValue is used in the JetStream offset timeline bucket as the value for the given processor entity key.
type OTValue struct {
	Offset    int64
	Watermark int64
}

// EncodeToBytes encodes a OTValue object into byte array.
func (v OTValue) EncodeToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeToOTValue decodes the given byte array into a OTValue object.
func DecodeToOTValue(b []byte) (OTValue, error) {
	buf := bytes.NewReader(b)
	var v OTValue
	err := binary.Read(buf, binary.LittleEndian, &v)
	if err != nil {
		return OTValue{}, err
	}
	return v, nil
}
