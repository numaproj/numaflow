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

package error

import (
	"fmt"
	"reflect"
	"testing"
)

func TestErrKind_String(t *testing.T) {
	tests := []struct {
		name string
		ek   ErrKind
		want string
	}{
		{
			name: "retry",
			ek:   0,
			want: "Retryable",
		},
		{
			name: "non-retry",
			ek:   1,
			want: "NonRetryable",
		},
		{
			name: "canceled",
			ek:   2,
			want: "Canceled",
		},
		{
			name: "unknown_2",
			ek:   3,
			want: "Unknown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ek.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFromError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name   string
		args   args
		want   *UDFError
		wantOk bool
	}{
		{
			name: "good_retry",
			args: args{
				err: &UDFError{
					errKind:    0,
					errMessage: "Retryable Error",
				},
			},
			want:   &UDFError{Retryable, "Retryable Error"},
			wantOk: true,
		},
		{
			name: "good_non_retry",
			args: args{
				err: &UDFError{
					errKind:    1,
					errMessage: "NonRetryable Error",
				},
			},
			want:   &UDFError{NonRetryable, "NonRetryable Error"},
			wantOk: true,
		},
		{
			name: "good_unknown_1",
			args: args{
				err: &UDFError{
					errKind:    3,
					errMessage: "Unknown Error",
				},
			},
			want:   &UDFError{Unknown, "Unknown Error"},
			wantOk: true,
		},
		{
			name: "good_not_standard",
			args: args{
				err: fmt.Errorf("not a standard error"),
			},
			want:   &UDFError{Unknown, "not a standard error"},
			wantOk: false,
		},
		{
			name: "good_canceled",
			args: args{
				err: &UDFError{
					errKind:    2,
					errMessage: "context canceled",
				},
			},
			want:   &UDFError{Canceled, "context canceled"},
			wantOk: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := FromError(tt.args.err)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FromError() gotK = %v, want %v", got, tt.want)
			}
			if gotOk != tt.wantOk {
				t.Errorf("FromError() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestUDFError_Error(t *testing.T) {
	type fields struct {
		ErrKind    ErrKind
		ErrMessage string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "retryable",
			fields: fields{
				ErrKind:    0,
				ErrMessage: "deadline exceeds",
			},
			want: "Retryable: deadline exceeds",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := UDFError{
				errKind:    tt.fields.ErrKind,
				errMessage: tt.fields.ErrMessage,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUDFError_ErrorKind(t *testing.T) {
	type fields struct {
		ErrKind    ErrKind
		ErrMessage string
	}
	tests := []struct {
		name   string
		fields fields
		want   ErrKind
	}{
		{
			name: "retryable",
			fields: fields{
				ErrKind:    0,
				ErrMessage: "deadline exceeds",
			},
			want: Retryable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := UDFError{
				errKind:    tt.fields.ErrKind,
				errMessage: tt.fields.ErrMessage,
			}
			if got := e.ErrorKind(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ErrorKind() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUDFError_ErrorMessage(t *testing.T) {
	type fields struct {
		ErrKind    ErrKind
		ErrMessage string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "retryable",
			fields: fields{
				ErrKind:    0,
				ErrMessage: "deadline exceeds",
			},
			want: "deadline exceeds",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := UDFError{
				errKind:    tt.fields.ErrKind,
				errMessage: tt.fields.ErrMessage,
			}
			if got := e.ErrorMessage(); got != tt.want {
				t.Errorf("ErrorMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}
