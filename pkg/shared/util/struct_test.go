package util

import "testing"

func TestIsZeroStruct(t *testing.T) {
	type args struct {
		v interface{}
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "zero_struct",
			args: args{v: struct {
				Name string
				Age  int
			}{}},
			want: true,
		},
		{
			name: "non_zero_struct",
			args: args{v: struct {
				Name string
				Age  int
			}{
				Name: "John",
				Age:  25,
			}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsZeroStruct(tt.args.v); got != tt.want {
				t.Errorf("IsZeroStruct() = %v, want %v", got, tt.want)
			}
		})
	}
}
