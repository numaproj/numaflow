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
	"reflect"
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	// Test cases
	cases := []struct {
		name string
		wmb  WMB
	}{
		{
			name: "idle_true",
			wmb:  WMB{Idle: true, Offset: 10, Watermark: 20, Partition: 1},
		},
		{
			name: "idle_false",
			wmb:  WMB{Idle: false, Offset: 100, Watermark: 200, Partition: 2},
		},
		{
			name: "empty_wmb",
			wmb:  WMB{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode the WMB struct to bytes
			encoded, err := tc.wmb.EncodeToBytes()
			if err != nil {
				t.Fatalf("Failed to encode: %v", err)
			}

			// Decode the bytes back to a WMB struct
			decoded, err := DecodeToWMB(encoded)
			if err != nil {
				t.Fatalf("Failed to decode: %v", err)
			}

			// The decoded WMB struct should be the same as the original
			if !reflect.DeepEqual(tc.wmb, decoded) {
				t.Errorf("Expected %+v, got %+v", tc.wmb, decoded)
			}
		})
	}
}

func TestDecodeToWMB_Error(t *testing.T) {
	// Provide an invalid byte array
	b := []byte{0xFF, 0xFF, 0xFF, 0xFF}

	_, err := DecodeToWMB(b)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}
