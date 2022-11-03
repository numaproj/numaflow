package otbucket

import (
	"bytes"
	"encoding/gob"
)

// OTValue is used in the JetStream offset timeline bucket as the value for the given processor entity key.
type OTValue struct {
	Offset    int64
	Watermark int64
}

func (v OTValue) EncodeToBytes() ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(v)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeToOTValue(b []byte) (OTValue, error) {
	v := OTValue{}
	dec := gob.NewDecoder(bytes.NewReader(b))
	err := dec.Decode(&v)
	if err != nil {
		return OTValue{}, err
	}
	return v, nil
}
