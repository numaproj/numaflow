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
	s := []string{"AAST/tOKxa8X2A40ZY0BAAAAAAAA",
		"ABK8cw6Sxa8XMK0zZY0BAAAAAAAA",
		"ABQDU7WTxa8XwAw1ZY0BAAAAAAAA",
		"ABwvJaSFxa8XyGozZY0BAAAAAAAA",
		"AC6ZWBeUxa8XALUzZY0BAAAAAAAA",
		"ACFAZQKVxa8XiMI0ZY0BAAAAAAAA",
		"ACGlPhWQxa8X0Dk0ZY0BAAAAAAAA",
		"ACO5iSWRxa8XGC40ZY0BAAAAAAAA",
		"ACfqIMyTxa8XsOU0ZY0BAAAAAAAA",
		"ACr/RJKLxa8XcEk0ZY0BAAAAAAAA",
		"ADJ6a0SKxa8XQM40ZY0BAAAAAAAA",
		"ADgvfx6Vxa8XMCQ1ZY0BAAAAAAAA",
		"ADwkhHqMxa8XQFE0ZY0BAAAAAAAA",
		"ADxz70qKxa8X4GA0ZY0BAAAAAAAA",
		"AEN2X2eJxa8XUH4zZY0BAAAAAAAA",
		"AEQHYc2Hxa8XgPMzZY0BAAAAAAAA",
		"AERBelyExa8XYCI0ZY0BAAAAAAAA",
		"AERZ7AWAxa8X6LgzZY0BAAAAAAAA",
		"AEhIQuGQxa8XUHg0ZY0BAAAAAAAA",
		"AEu61KuHxa8XuMYyZY0BAAAAAAAA",
		"AF9NCC+Vxa8XQM40ZY0BAAAAAAAA",
		"AFiwc0KPxa8XqJkzZY0BAAAAAAAA",
		"AGHBEpCRxa8X+Nk0ZY0BAAAAAAAA",
		"AGLXU2yKxa8XEFk0ZY0BAAAAAAAA",
		"AGLlJRqVxa8XYBw1ZY0BAAAAAAAA",
		"AGNLZkGTxa8XmOk0ZY0BAAAAAAAA",
		"AGQTXC6Pxa8XENY0ZY0BAAAAAAAA",
		"AGgV/C+Wxa8XOP8zZY0BAAAAAAAA",
		"AH9UK0CBxa8XyOczZY0BAAAAAAAA",
		"AHIC4VF+xa8XOP8zZY0BAAAAAAAA",
		"AHYCWIOIxa8X0D8zZY0BAAAAAAAA",
		"AHdBFcOPxa8XUPU0ZY0BAAAAAAAA",
		"AHeh2E2Cxa8XmO8zZY0BAAAAAAAA",
		"AHjtVv6Txa8XKNI0ZY0BAAAAAAAA",
		"AHr2cF+Uxa8XUPU0ZY0BAAAAAAAA",
		"AI9Di3qMxa8XADgzZY0BAAAAAAAA",
		"AIJw/uiFxa8XgHA0ZY0BAAAAAAAA",
		"AISEd2iExa8XqBY0ZY0BAAAAAAAA",
		"AIbu/IKUxa8XgHA0ZY0BAAAAAAAA",
		"AJ4VgKx/xa8X0LwzZY0BAAAAAAAA",
		"AJSrCcuTxa8XCAE1ZY0BAAAAAAAA",
		"AJTnXuKUxa8XUPU0ZY0BAAAAAAAA",
		"AJekNrx7xa8XmHIzZY0BAAAAAAAA",
		"AKAju8yLxa8XqJkzZY0BAAAAAAAA",
		"AKEvktV9xa8XENwzZY0BAAAAAAAA",
		"AKLopmyRxa8XADI0ZY0BAAAAAAAA",
		"AKT7kueTxa8X2Ag1ZY0BAAAAAAAA",
		"AKZH4muGxa8XSCY0ZY0BAAAAAAAA",
		"AKciM9iQxa8XqJM0ZY0BAAAAAAAA",
		"AKgE0/uSxa8XsGg0ZY0BAAAAAAAA",
		"AKhg9eSSxa8XsGg0ZY0BAAAAAAAA",
		"ALxfe0CVxa8XWMo0ZY0BAAAAAAAA",
		"AM5x49OKxa8XqJM0ZY0BAAAAAAAA",
		"AM7PfsWPxa8XeB40ZY0BAAAAAAAA",
		"AMCXB5OHxa8XcMwzZY0BAAAAAAAA",
		"AMOVZuKRxa8XGC40ZY0BAAAAAAAA",
		"AMTfQFeSxa8X6C81ZY0BAAAAAAAA",
		"ANJzi3GMxa8XaHQ0ZY0BAAAAAAAA",
		"ANZ6ged9xa8XSKkzZY0BAAAAAAAA",
		"ANrgLFGMxa8XCIQ0ZY0BAAAAAAAA",
		"ANvxaIyFxa8XwBI0ZY0BAAAAAAAA",
		"AOOBzkOUxa8XACw1ZY0BAAAAAAAA",
		"AOnJVL+Pxa8XqJkzZY0BAAAAAAAA",
		"AOwjWNaVxa8XYCI0ZY0BAAAAAAAA",
		"AP25y9WExa8XcEk0ZY0BAAAAAAAA",
		"APFEpGOUxa8XwAw1ZY0BAAAAAAAA",
		"APIl0DKUxa8XqBA1ZY0BAAAAAAAA",
		"APJxd6WUxa8X2A40ZY0BAAAAAAAA",
		"APLO7vuJxa8XoEE0ZY0BAAAAAAAA",
		"APNzYBV8xa8XqJkzZY0BAAAAAAAA",
		"APX8PkZ1xa8X6L4yZY0BAAAAAAAA",
		"APonVdqUxa8X8Ic0ZY0BAAAAAAAA"}

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
