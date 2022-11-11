package ot

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestDecodeToOTValue(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name    string
		args    args
		want    Value
		wantErr bool
	}{
		{
			name: "decode_success_using_ot_value",
			args: args{
				b: func() []byte {
					v := Value{
						Offset:    100,
						Watermark: 1667495100000,
					}
					buf := new(bytes.Buffer)
					_ = binary.Write(buf, binary.LittleEndian, v)
					return buf.Bytes()
				}(),
			},
			want: Value{
				Offset:    100,
				Watermark: 1667495100000,
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
			want:    Value{},
			wantErr: true,
		},
		{
			name: "decode_success_using_2_field_struct",
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
			want: Value{
				Offset:    100,
				Watermark: 1667495100000,
			},
			wantErr: false,
		},
		{
			name: "decode_success_using_3_field_struct",
			args: args{
				b: func() []byte {
					v := struct {
						Test0 int64
						Test1 int64
						Test2 int64 // should be ignored
					}{
						Test0: 100,
						Test1: 1667495100000,
						Test2: 200,
					}
					buf := new(bytes.Buffer)
					_ = binary.Write(buf, binary.LittleEndian, v)
					return buf.Bytes()
				}(),
			},
			want: Value{
				Offset:    100,
				Watermark: 1667495100000,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeToOTValue(tt.args.b)
			if (err != nil) != tt.wantErr {
				t.Errorf("DecodeToOTValue() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DecodeToOTValue() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOTValue_EncodeToBytes(t *testing.T) {
	// bytes.Buffer Write doesn't return err, so err is always nil
	type fields struct {
		Offset    int64
		Watermark int64
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
				Offset:    100,
				Watermark: 1667495100000,
			},
			want:    []byte{100, 0, 0, 0, 0, 0, 0, 0, 96, 254, 115, 62, 132, 1, 0, 0},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Value{
				Offset:    tt.fields.Offset,
				Watermark: tt.fields.Watermark,
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
