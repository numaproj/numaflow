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
	// Idle is set to true if the given processor entity hasn't published anything
	// to the offset timeline bucket in a batch processing cycle.
	// Idle is used to signal an idle watermark.
	Idle bool
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
