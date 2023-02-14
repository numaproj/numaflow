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

import "encoding/json"

// MarshalBinary encodes header to a binary format
func (h *Header) MarshalBinary() (data []byte, err error) {
	return json.Marshal(h)
}

// UnmarshalBinary decodes header from the binary format
func (h *Header) UnmarshalBinary(data []byte) (err error) {
	return json.Unmarshal(data, &h)
}

// MarshalBinary encodes header to a binary format
func (b *Body) MarshalBinary() (data []byte, err error) {
	return json.Marshal(b)
}

// UnmarshalBinary decodes header from the binary format
func (b *Body) UnmarshalBinary(data []byte) (err error) {
	return json.Unmarshal(data, &b)
}

// MarshalBinary encodes Message to the binary format
func (m *Message) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)
}

// UnmarshalBinary decodes Message from the binary format
func (m *Message) UnmarshalBinary(data []byte) (err error) {
	return json.Unmarshal(data, &m)
}

// MarshalBinary encodes ReadMessage to the binary format
func (r *ReadMessage) MarshalBinary() (data []byte, err error) {
	return json.Marshal(r)
}

// UnmarshalBinary decodes ReadMessage from the binary format
func (r *ReadMessage) UnmarshalBinary(data []byte) (err error) {
	return json.Unmarshal(data, &r)
}
