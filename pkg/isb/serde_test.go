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
	"reflect"
	"testing"
	"time"
)

func TestHeader(t *testing.T) {
	type fields struct {
		MessageInfo MessageInfo
		Kind        MessageKind
		ID          MessageID
		Key         []string
		Headers     map[string]string
	}
	tests := []struct {
		name               string
		fields             fields
		wantData           Header
		wantMarshalError   bool
		wantUnmarshalError bool
	}{
		{
			name: "good",
			fields: fields{
				MessageInfo: MessageInfo{
					EventTime: time.UnixMilli(1676617200000),
					IsLate:    true,
				},
				Kind: Data,
				ID: MessageID{
					VertexName: "test-vertex",
					Offset:     "test-offset",
					Index:      0,
				},
				Key: []string{"TestKey", "TestKey2"},
			},
			wantData: Header{
				MessageInfo: MessageInfo{
					EventTime: time.UnixMilli(1676617200000).UTC(),
					IsLate:    true,
				},
				Kind: Data,
				ID: MessageID{
					VertexName: "test-vertex",
					Offset:     "test-offset",
					Index:      0,
				},
				Keys: []string{"TestKey", "TestKey2"},
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
		{
			name: "good_with_headers",
			fields: fields{
				MessageInfo: MessageInfo{
					EventTime: time.UnixMilli(1676617200000),
					IsLate:    true,
				},
				Kind: Data,
				ID: MessageID{
					VertexName: "test-vertex",
					Offset:     "test-offset",
					Index:      0,
				},
				Key: []string{"TestKey", "TestKey2"},
				Headers: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
			},
			wantData: Header{
				MessageInfo: MessageInfo{
					EventTime: time.UnixMilli(1676617200000).UTC(),
					IsLate:    true,
				},
				Kind: Data,
				ID: MessageID{
					VertexName: "test-vertex",
					Offset:     "test-offset",
					Index:      0,
				},
				Keys: []string{"TestKey", "TestKey2"},
				Headers: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Header{
				MessageInfo: tt.fields.MessageInfo,
				Kind:        tt.fields.Kind,
				ID:          tt.fields.ID,
				Keys:        tt.fields.Key,
				Headers:     tt.fields.Headers,
			}
			gotData, err := h.MarshalBinary()
			if (err != nil) != tt.wantMarshalError {
				t.Errorf("MarshalBinary() error = %v, wantMarshalError %v", err, tt.wantMarshalError)
				return
			}
			var newH = new(Header)
			err = newH.UnmarshalBinary(gotData)
			if (err != nil) != tt.wantUnmarshalError {
				t.Errorf("UnmarshalBinary() error = %v, wantUnmarshalError %v", err, tt.wantMarshalError)
				return
			}
			if !reflect.DeepEqual(*newH, tt.wantData) {
				t.Errorf("MarshalBinary() gotData = %v, want %v", newH, tt.wantData)
			}
		})
	}
}

func TestMessage(t *testing.T) {
	type fields struct {
		Header Header
		Body   Body
	}
	tests := []struct {
		name               string
		fields             fields
		wantData           Message
		wantMarshalError   bool
		wantUnmarshalError bool
	}{
		{
			name: "good",
			fields: fields{
				Header: Header{
					MessageInfo: MessageInfo{
						EventTime: time.UnixMilli(1676617200000),
						IsLate:    true,
					},
					Kind: Data,
					ID: MessageID{
						VertexName: "test-vertex",
						Offset:     "test-offset",
						Index:      0,
					},
					Keys: []string{"TestKey"},
				},
				Body: Body{
					Payload: []byte("TestBODY"),
				},
			},
			wantData: Message{
				Header: Header{
					MessageInfo: MessageInfo{
						EventTime: time.UnixMilli(1676617200000).UTC(),
						IsLate:    true,
					},
					Kind: Data,
					ID: MessageID{
						VertexName: "test-vertex",
						Offset:     "test-offset",
						Index:      0,
					},
					Keys: []string{"TestKey"},
				},
				Body: Body{
					Payload: []byte("TestBODY"),
				},
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
		{
			name:               "good_empty",
			fields:             fields{},
			wantData:           Message{},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
		{
			name: "good_with_headers",
			fields: fields{
				Header: Header{
					MessageInfo: MessageInfo{
						EventTime: time.UnixMilli(1676617200000),
						IsLate:    true,
					},
					Kind: Data,
					ID: MessageID{
						VertexName: "test-vertex",
						Offset:     "test-offset",
						Index:      0,
					},
					Keys: []string{"TestKey"},
					Headers: map[string]string{
						"key1": "value1",
						"key2": "value2",
					},
				},
				Body: Body{
					Payload: []byte("TestBODY"),
				},
			},
			wantData: Message{
				Header: Header{
					MessageInfo: MessageInfo{
						EventTime: time.UnixMilli(1676617200000).UTC(),
						IsLate:    true,
					},
					Kind: Data,
					ID: MessageID{
						VertexName: "test-vertex",
						Offset:     "test-offset",
						Index:      0,
					},
					Keys: []string{"TestKey"},
					Headers: map[string]string{
						"key1": "value1",
						"key2": "value2",
					},
				},
				Body: Body{
					Payload: []byte("TestBODY"),
				},
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Message{
				Header: tt.fields.Header,
				Body:   tt.fields.Body,
			}
			gotData, err := m.MarshalBinary()
			if (err != nil) != tt.wantMarshalError {
				t.Errorf("MarshalBinary() error = %v, wantMarshalError %v", err, tt.wantMarshalError)
				return
			}
			var newM = new(Message)
			err = newM.UnmarshalBinary(gotData)
			if (err != nil) != tt.wantUnmarshalError {
				t.Errorf("UnmarshalBinary() error = %v, wantUnmarshalError %v", err, tt.wantMarshalError)
				return
			}
			if !reflect.DeepEqual(*newM, tt.wantData) {
				t.Errorf("MarshalBinary() gotData = %v, want %v", newM, &tt.wantData)
			}
		})
	}
}
