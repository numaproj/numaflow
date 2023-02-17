package isb

import (
	"fmt"
	"testing"
	"time"
)

func TestPaneInfo_MarshalBinary(t *testing.T) {
	type fields struct {
		EventTime time.Time
		StartTime time.Time
		EndTime   time.Time
		IsLate    bool
	}
	tests := []struct {
		name     string
		fields   fields
		wantData []byte
		wantErr  bool
	}{
		{
			name: "good",
			fields: fields{
				EventTime: time.UnixMilli(1676617200000),
				IsLate:    true,
			},
			wantData: nil,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PaneInfo{
				EventTime: tt.fields.EventTime,
				StartTime: tt.fields.StartTime,
				EndTime:   tt.fields.EndTime,
				IsLate:    tt.fields.IsLate,
			}
			gotData, _ := p.MarshalBinary()
			fmt.Println(gotData)
			// if (err != nil) != tt.wantErr {
			// 	t.Errorf("MarshalBinary() error = %v, wantErr %v", err, tt.wantErr)
			// 	return
			// }
			// if !reflect.DeepEqual(gotData, tt.wantData) {
			// 	t.Errorf("MarshalBinary() gotData = %v, want %v", gotData, tt.wantData)
			// }
			var newP = new(PaneInfo)
			_ = newP.UnmarshalBinary(gotData)
			fmt.Println(newP.EventTime, newP.IsLate)
		})
	}
}

func TestHeader_MarshalBinary(t *testing.T) {
	type fields struct {
		PaneInfo PaneInfo
		ID       string
		Key      string
	}
	tests := []struct {
		name     string
		fields   fields
		wantData []byte
		wantErr  bool
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
			wantData: nil,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Header{
				PaneInfo: tt.fields.PaneInfo,
				ID:       tt.fields.ID,
				Key:      tt.fields.Key,
			}
			gotData, _ := h.MarshalBinary()
			fmt.Println(gotData)
			var newH = new(Header)
			_ = newH.UnmarshalBinary(gotData)
			fmt.Println(newH.PaneInfo.EventTime, newH.PaneInfo.IsLate, newH.ID, newH.Key)
		})
	}
}

func TestMessage_MarshalBinary(t *testing.T) {
	type fields struct {
		Header Header
		Body   Body
	}
	tests := []struct {
		name     string
		fields   fields
		wantData []byte
		wantErr  bool
	}{
		// TODO: Add test cases.
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
			wantData: nil,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Message{
				Header: tt.fields.Header,
				Body:   tt.fields.Body,
			}
			gotData, _ := m.MarshalBinary()
			fmt.Println(gotData)
			// if (err != nil) != tt.wantErr {
			// 	t.Errorf("MarshalBinary() error = %v, wantErr %v", err, tt.wantErr)
			// 	return
			// }
			// if !reflect.DeepEqual(gotData, tt.wantData) {
			// 	t.Errorf("MarshalBinary() gotData = %v, want %v", gotData, tt.wantData)
			// }
			var newMsg = new(Message)
			_ = newMsg.UnmarshalBinary(gotData)
			fmt.Println(newMsg.Header.PaneInfo.EventTime, newMsg.Header.PaneInfo.IsLate, newMsg.Header.ID, newMsg.Header.Key, string(newMsg.Body.Payload))
		})
	}
}

func TestReadMessage_MarshalBinary(t *testing.T) {
	type fields struct {
		Message    Message
		ReadOffset Offset
		Watermark  time.Time
	}
	tests := []struct {
		name     string
		fields   fields
		wantData []byte
		wantErr  bool
	}{
		// TODO: Add test cases.
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
			wantData: nil,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rm := &ReadMessage{
				Message:    tt.fields.Message,
				ReadOffset: tt.fields.ReadOffset,
				Watermark:  tt.fields.Watermark,
			}
			gotData, _ := rm.MarshalBinary()
			fmt.Println(gotData)
			// if (err != nil) != tt.wantErr {
			// 	t.Errorf("MarshalBinary() error = %v, wantErr %v", err, tt.wantErr)
			// 	return
			// }
			// if !reflect.DeepEqual(gotData, tt.wantData) {
			// 	t.Errorf("MarshalBinary() gotData = %v, want %v", gotData, tt.wantData)
			// }
			var newReadMsg = new(ReadMessage)
			_ = newReadMsg.UnmarshalBinary(gotData)
			offset, _ := newReadMsg.ReadOffset.Sequence()
			fmt.Println(newReadMsg.Header.PaneInfo.EventTime, newReadMsg.Header.PaneInfo.IsLate, newReadMsg.Header.ID, newReadMsg.Header.Key, string(newReadMsg.Body.Payload), offset, newReadMsg.Watermark)
		})
	}
}
