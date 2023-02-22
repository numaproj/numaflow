package isb

import (
	"reflect"
	"testing"
	"time"
)

func TestPaneInfo(t *testing.T) {
	type fields struct {
		EventTime time.Time
		StartTime time.Time
		EndTime   time.Time
		IsLate    bool
	}
	tests := []struct {
		name               string
		fields             fields
		wantData           PaneInfo
		wantMarshalError   bool
		wantUnmarshalError bool
	}{
		{
			name: "good",
			fields: fields{
				EventTime: time.UnixMilli(1676617200000),
				IsLate:    false,
			},
			wantData: PaneInfo{
				EventTime: time.UnixMilli(1676617200000).UTC(),
				IsLate:    false,
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
		{
			name: "good_is_late",
			fields: fields{
				EventTime: time.UnixMilli(1676617200000),
				IsLate:    true,
			},
			wantData: PaneInfo{
				EventTime: time.UnixMilli(1676617200000).UTC(),
				IsLate:    true,
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PaneInfo{
				EventTime: tt.fields.EventTime,
				IsLate:    tt.fields.IsLate,
			}
			gotData, err := p.MarshalBinary()
			if (err != nil) != tt.wantMarshalError {
				t.Errorf("MarshalBinary() error = %v, wantMarshalError %v", err, tt.wantMarshalError)
				return
			}
			var newP = new(PaneInfo)
			err = newP.UnmarshalBinary(gotData)
			if (err != nil) != tt.wantUnmarshalError {
				t.Errorf("UnmarshalBinary() error = %v, wantUnmarshalError %v", err, tt.wantMarshalError)
				return
			}
			if !reflect.DeepEqual(*newP, tt.wantData) {
				t.Errorf("MarshalBinary() gotData = %v, want %v", newP, tt.wantData)
			}
		})
	}
}

func TestHeader(t *testing.T) {
	type fields struct {
		PaneInfo PaneInfo
		ID       string
		Key      string
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
				PaneInfo: PaneInfo{
					EventTime: time.UnixMilli(1676617200000),
					IsLate:    true,
				},
				ID:  "TestID",
				Key: "TestKey",
			},
			wantData: Header{
				PaneInfo: PaneInfo{
					EventTime: time.UnixMilli(1676617200000).UTC(),
					IsLate:    true,
				},
				ID:  "TestID",
				Key: "TestKey",
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Header{
				PaneInfo: tt.fields.PaneInfo,
				ID:       tt.fields.ID,
				Key:      tt.fields.Key,
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

func TestBody(t *testing.T) {
	type fields struct {
		Payload []byte
	}
	tests := []struct {
		name               string
		fields             fields
		wantData           Body
		wantMarshalError   bool
		wantUnmarshalError bool
	}{
		{
			name: "good",
			fields: fields{
				Payload: []byte("TestBODY"),
			},
			wantData: Body{
				Payload: []byte("TestBODY"),
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
		{
			name:   "good_empty",
			fields: fields{},
			wantData: Body{
				Payload: nil,
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Body{
				Payload: tt.fields.Payload,
			}
			gotData, err := m.MarshalBinary()
			if (err != nil) != tt.wantMarshalError {
				t.Errorf("MarshalBinary() error = %v, wantMarshalError %v", err, tt.wantMarshalError)
				return
			}
			var newB = new(Body)
			err = newB.UnmarshalBinary(gotData)
			if (err != nil) != tt.wantUnmarshalError {
				t.Errorf("UnmarshalBinary() error = %v, wantUnmarshalError %v", err, tt.wantMarshalError)
				return
			}
			if !reflect.DeepEqual(*newB, tt.wantData) {
				t.Errorf("MarshalBinary() gotData = %v, want %v", newB, tt.wantData)
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
					PaneInfo: PaneInfo{
						EventTime: time.UnixMilli(1676617200000),
						IsLate:    true,
					},
					ID:  "TestID",
					Key: "TestKey",
				},
				Body: Body{
					Payload: []byte("TestBODY"),
				},
			},
			wantData: Message{
				Header: Header{
					PaneInfo: PaneInfo{
						EventTime: time.UnixMilli(1676617200000).UTC(),
						IsLate:    true,
					},
					ID:  "TestID",
					Key: "TestKey",
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
				t.Errorf("MarshalBinary() gotData = %v, want %v", newM, tt.wantData)
			}
		})
	}
}

func TestReadMessage(t *testing.T) {
	type fields struct {
		Message    Message
		ReadOffset Offset
		Watermark  time.Time
	}
	tests := []struct {
		name               string
		fields             fields
		wantData           ReadMessage
		wantMarshalError   bool
		wantUnmarshalError bool
	}{
		{
			name: "good",
			fields: fields{
				Message: Message{
					Header: Header{
						PaneInfo: PaneInfo{
							EventTime: time.UnixMilli(1676617200000),
							IsLate:    true,
						},
						ID:  "TestID",
						Key: "TestKey",
					},
					Body: Body{
						Payload: []byte("TestBODY"),
					},
				},
				ReadOffset: SimpleIntOffset(func() int64 {
					return 123
				}),
				Watermark: time.UnixMilli(1676613600000),
			},
			wantData: ReadMessage{
				Message: Message{
					Header: Header{
						PaneInfo: PaneInfo{
							EventTime: time.UnixMilli(1676617200000).UTC(),
							IsLate:    true,
						},
						ID:  "TestID",
						Key: "TestKey",
					},
					Body: Body{
						Payload: []byte("TestBODY"),
					},
				},
				ReadOffset: SimpleIntOffset(func() int64 {
					return 123
				}),
				Watermark: time.UnixMilli(1676613600000).UTC(),
			},
			wantMarshalError:   false,
			wantUnmarshalError: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm := &ReadMessage{
				Message:    tt.fields.Message,
				ReadOffset: tt.fields.ReadOffset,
				Watermark:  tt.fields.Watermark,
			}
			gotData, err := rm.MarshalBinary()
			if (err != nil) != tt.wantMarshalError {
				t.Errorf("MarshalBinary() error = %v, wantMarshalError %v", err, tt.wantMarshalError)
				return
			}
			var newRM = new(ReadMessage)
			err = newRM.UnmarshalBinary(gotData)
			if (err != nil) != tt.wantUnmarshalError {
				t.Errorf("UnmarshalBinary() error = %v, wantUnmarshalError %v", err, tt.wantMarshalError)
				return
			}
			if !reflect.DeepEqual((*newRM).Message, tt.wantData.Message) &&
				!reflect.DeepEqual((*newRM).Watermark, tt.wantData.Watermark) &&
				!reflect.DeepEqual((*newRM).ReadOffset.String(), tt.wantData.ReadOffset.String()) {
				t.Errorf("MarshalBinary() gotData = %v, want %v", newRM, tt.wantData)
			}
		})
	}
}
