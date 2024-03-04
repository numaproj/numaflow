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

package wmb

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"reflect"
	"testing"
	"time"
)

func TestDecodeToWMB(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name    string
		args    args
		want    WMB
		wantErr bool
	}{
		{
			name: "decode_success_using_ot_value",
			args: args{
				b: func() []byte {
					v := WMB{
						Offset:    100,
						Watermark: 1667495100000,
						Idle:      false,
					}
					buf := new(bytes.Buffer)
					_ = binary.Write(buf, binary.LittleEndian, v)
					return buf.Bytes()
				}(),
			},
			want: WMB{
				Offset:    100,
				Watermark: 1667495100000,
				Idle:      false,
			},
			wantErr: false,
		},
		{
			name: "decode_failure_using_1_field_struct",
			args: args{
				b: func() []byte {
					v := struct {
						Test int64
					}{
						Test: 100,
					}
					buf := new(bytes.Buffer)
					_ = binary.Write(buf, binary.LittleEndian, v)
					return buf.Bytes()
				}(),
			},
			want:    WMB{},
			wantErr: true,
		},
		{
			name: "decode_failure_using_2_field_struct",
			args: args{
				b: func() []byte {
					v := struct {
						Test0 int64
						Test1 int64
					}{
						Test0: 100,
						Test1: 1667495100000,
					}
					buf := new(bytes.Buffer)
					_ = binary.Write(buf, binary.LittleEndian, v)
					return buf.Bytes()
				}(),
			},
			want:    WMB{},
			wantErr: true,
		},
		{
			name: "decode_success_using_4_field_struct",
			args: args{
				b: func() []byte {
					v := struct {
						Test0 bool
						Test1 int64
						Test2 int64
						Test3 int32
					}{
						Test0: true,
						Test1: 0,
						Test2: 0,
						Test3: 0,
					}
					buf := new(bytes.Buffer)
					_ = binary.Write(buf, binary.LittleEndian, v)
					return buf.Bytes()
				}(),
			},
			want: WMB{
				Offset:    0,
				Watermark: 0,
				Idle:      true,
				Partition: 0,
			},
			wantErr: false,
		},
		{
			name: "decode_success_using_5_field_struct",
			args: args{
				b: func() []byte {
					v := struct {
						Test0 bool
						Test1 int64
						Test2 int64
						Test3 int32
						Test4 int32 // should be ignored
					}{
						Test0: false,
						Test1: 100,
						Test2: 1667495100000,
						Test3: 3,
						Test4: 20,
					}
					buf := new(bytes.Buffer)
					_ = binary.Write(buf, binary.LittleEndian, v)
					return buf.Bytes()
				}(),
			},
			want: WMB{
				Offset:    100,
				Watermark: 1667495100000,
				Idle:      false,
				Partition: 3,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeToWMB(tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeToWMB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DecodeToWMB() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWMB_EncodeToBytes(t *testing.T) {
	// bytes.Buffer Write doesn't return err, so err is always nil
	type fields struct {
		Offset    int64
		Watermark int64
		Idle      bool
		Partition int32
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{
			name: "encode_success",
			fields: fields{
				Idle:      false,
				Offset:    100,
				Watermark: 1667495100000,
				Partition: 3,
			},
			want:    []byte{0, 100, 0, 0, 0, 0, 0, 0, 0, 96, 254, 115, 62, 132, 1, 0, 0, 3, 0, 0, 0},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := WMB{
				Offset:    tt.fields.Offset,
				Watermark: tt.fields.Watermark,
				Partition: tt.fields.Partition,
			}
			got, err := v.EncodeToBytes()
			if (err != nil) != tt.wantErr {
				t.Errorf("EncodeToBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeToBytes() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeToWMB2(t *testing.T) {
	s := []string{"ABdAroOUX6gX2GLZ34wBAAAAAAAA", "AJ3Im8aYX6gXYCf734wBAAAAAAAA", "ACIrAAKXX6gXoI/a34wBAAAAAAAA"}

	for _, v := range s {

		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			t.Errorf("DecodeToWMB() error = %v", err)
			return
		}

		got, err := DecodeToWMB(b)
		if err != nil {
			t.Errorf("DecodeToWMB() error = %v", err)
			return
		}

		println(int32(time.Since(time.UnixMilli(got.Watermark)).Minutes()))
	}
}
