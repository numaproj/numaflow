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

package isb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"
)

type paneInfoPreamble struct {
	EventEpoch int64
	IsLate     bool
}

func (p PaneInfo) MarshalBinary() (data []byte, err error) {
	var buf = new(bytes.Buffer)
	var preamble = paneInfoPreamble{
		EventEpoch: p.EventTime.UnixMilli(),
		IsLate:     p.IsLate,
	}
	if err = binary.Write(buf, binary.LittleEndian, preamble); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *PaneInfo) UnmarshalBinary(data []byte) (err error) {
	var r = bytes.NewReader(data)
	var preamble = new(paneInfoPreamble)
	err = binary.Read(r, binary.LittleEndian, preamble)
	if err != nil {
		return err
	}
	p.EventTime = time.UnixMilli(preamble.EventEpoch).UTC()
	p.IsLate = preamble.IsLate
	return nil
}

type headerPreamble struct {
	PLen   int16
	IDLen  int16
	KeyLen int16
}

// MarshalBinary encodes header to a binary format
func (h Header) MarshalBinary() (data []byte, err error) {
	var buf = new(bytes.Buffer)
	paneInfo, err := h.PaneInfo.MarshalBinary()
	if err != nil {
		return nil, err
	}
	var preamble = headerPreamble{
		PLen:   int16(len(paneInfo)),
		IDLen:  int16(len(h.ID)),
		KeyLen: int16(len(h.Key)),
	}
	if err = binary.Write(buf, binary.LittleEndian, preamble); err != nil {
		return nil, err
	}
	n, err := buf.Write(paneInfo)
	if err != nil {
		return nil, err
	} else if n != int(preamble.PLen) {
		return nil, fmt.Errorf("expected to write paneInfo size of %d but got %d", preamble.PLen, n)
	}
	if err = binary.Write(buf, binary.LittleEndian, []byte(h.ID)); err != nil {
		return nil, err
	}
	if err = binary.Write(buf, binary.LittleEndian, []byte(h.Key)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary decodes header from the binary format
func (h *Header) UnmarshalBinary(data []byte) (err error) {
	var r = bytes.NewReader(data)
	var preamble = new(headerPreamble)
	if err = binary.Read(r, binary.LittleEndian, preamble); err != nil {
		return err
	}
	var paneInfoByte = make([]byte, preamble.PLen)
	n, err := r.Read(paneInfoByte)
	if err != nil {
		return err
	} else if n != int(preamble.PLen) {
		return fmt.Errorf("expected to read paneInfo size of %d but got %d", preamble.PLen, n)
	}
	var paneInfo = new(PaneInfo)
	if err = paneInfo.UnmarshalBinary(paneInfoByte); err != nil {
		return err
	}
	var id = make([]byte, preamble.IDLen)
	if err = binary.Read(r, binary.LittleEndian, id); err != nil {
		return err
	}
	var key = make([]byte, preamble.KeyLen)
	if err = binary.Read(r, binary.LittleEndian, key); err != nil {
		return err
	}
	h.PaneInfo = *paneInfo
	h.ID = string(id)
	h.Key = string(key)
	return err
}

type bodyPreamble struct {
	PLen int64
}

// MarshalBinary encodes header to a binary format
func (b Body) MarshalBinary() (data []byte, err error) {
	var buf = new(bytes.Buffer)
	var preamble = bodyPreamble{
		PLen: int64(len(b.Payload)),
	}
	if err = binary.Write(buf, binary.LittleEndian, preamble); err != nil {
		return nil, err
	}
	if err = binary.Write(buf, binary.LittleEndian, b.Payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary decodes header from the binary format
func (b *Body) UnmarshalBinary(data []byte) (err error) {
	var r = bytes.NewReader(data)
	var preamble = new(bodyPreamble)
	if err = binary.Read(r, binary.LittleEndian, preamble); err != nil {
		return err
	}
	if preamble.PLen != 0 {
		var Payload = make([]byte, preamble.PLen)
		if err = binary.Read(r, binary.LittleEndian, Payload); err != nil {
			return err
		}
		b.Payload = Payload
	}
	return err
}

type messagePreamble struct {
	HLen int16
	BLen int16
}

// MarshalBinary encodes Message to the binary format
func (m Message) MarshalBinary() (data []byte, err error) {
	var buf = new(bytes.Buffer)
	header, err := m.Header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	body, err := m.Body.MarshalBinary()
	if err != nil {
		return nil, err
	}
	var preamble = messagePreamble{
		HLen: int16(len(header)),
		BLen: int16(len(body)),
	}
	if err = binary.Write(buf, binary.LittleEndian, preamble); err != nil {
		return nil, err
	}
	n, err := buf.Write(header)
	if err != nil {
		return nil, err
	} else if n != int(preamble.HLen) {
		return nil, fmt.Errorf("expected to write header size of %d but got %d", preamble.HLen, n)
	}
	n, err = buf.Write(body)
	if err != nil {
		return nil, err
	} else if n != int(preamble.BLen) {
		return nil, fmt.Errorf("expected to write body size of %d but got %d", preamble.BLen, n)
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary decodes Message from the binary format
func (m *Message) UnmarshalBinary(data []byte) (err error) {
	var r = bytes.NewReader(data)
	var preamble = new(messagePreamble)
	if err = binary.Read(r, binary.LittleEndian, preamble); err != nil {
		return err
	}
	var headerByte = make([]byte, preamble.HLen)
	n, err := r.Read(headerByte)
	if err != nil {
		return err
	} else if n != int(preamble.HLen) {
		return fmt.Errorf("expected to read header size of %d but got %d", preamble.HLen, n)
	}
	var header = new(Header)
	if err = header.UnmarshalBinary(headerByte); err != nil {
		return err
	}
	var bodyByte = make([]byte, preamble.BLen)
	n, err = r.Read(bodyByte)
	if err != nil {
		return err
	} else if n != int(preamble.BLen) {
		return fmt.Errorf("expected to read body size of %d but got %d", preamble.BLen, n)
	}
	var body = new(Body)
	if err = body.UnmarshalBinary(bodyByte); err != nil {
		return err
	}
	m.Header = *header
	m.Body = *body
	return err
}

type readMessagePreamble struct {
	MLen int16
	// TODO: currently only support simple int offset
	SimpleIntOffset int64
	WMEpoch         int64
}

// MarshalBinary encodes ReadMessage to the binary format
func (rm ReadMessage) MarshalBinary() (data []byte, err error) {
	var buf = new(bytes.Buffer)
	message, err := rm.Message.MarshalBinary()
	if err != nil {
		return nil, err
	}
	var offset int64
	switch rm.ReadOffset.(type) {
	case SimpleIntOffset:
		offset, err = rm.ReadOffset.Sequence()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("currently only support SimpleIntOffset")
	}
	var preamble = readMessagePreamble{
		MLen:            int16(len(message)),
		SimpleIntOffset: offset,
		WMEpoch:         rm.Watermark.UnixMilli(),
	}
	if err = binary.Write(buf, binary.LittleEndian, preamble); err != nil {
		return nil, err
	}
	n, err := buf.Write(message)
	if err != nil {
		return nil, err
	} else if n != int(preamble.MLen) {
		return nil, fmt.Errorf("expected to write message size of %d but got %d", preamble.MLen, n)
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary decodes ReadMessage from the binary format
func (rm *ReadMessage) UnmarshalBinary(data []byte) (err error) {
	var r = bytes.NewReader(data)
	var preamble = new(readMessagePreamble)
	if err = binary.Read(r, binary.LittleEndian, preamble); err != nil {
		return err
	}
	var messageByte = make([]byte, preamble.MLen)
	n, err := r.Read(messageByte)
	if err != nil {
		return err
	} else if n != int(preamble.MLen) {
		return fmt.Errorf("expected to read message size of %d but got %d", preamble.MLen, n)
	}
	var message = new(Message)
	if err = message.UnmarshalBinary(messageByte); err != nil {
		return err
	}

	rm.Message = *message
	rm.ReadOffset = SimpleIntOffset(func() int64 {
		return preamble.SimpleIntOffset
	})
	rm.Watermark = time.UnixMilli(preamble.WMEpoch).UTC()
	return err
}
