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

package processor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEntity(t *testing.T) {
	e := NewProcessorEntity("pod0")
	assert.Equal(t, "pod0", e.GetID())
}

func ExampleWatermark_String() {
	location, _ := time.LoadLocation("UTC")
	wm := Watermark(time.Unix(1651129200, 0).In(location))
	fmt.Println(wm)
	// output:
	// 2022-04-28T07:00:00Z
}

func TestExampleWatermarkUnix(t *testing.T) {
	wm := Watermark(time.UnixMilli(1651129200000))
	assert.Equal(t, int64(1651129200000), wm.UnixMilli())
}

func TestProcessorEntity_ParseOTWatcherKey(t *testing.T) {
	tests := []struct {
		name      string
		p         *ProcessorEntity
		arg       string
		wantEpoch int64
		wantSkip  bool
		wantErr   assert.ErrorAssertionFunc
	}{
		{
			name:      "bad_without_split_butSkip",
			p:         NewProcessorEntity("test1"),
			arg:       _defaultKeySeparator + "1234", // name is missing
			wantEpoch: 1234,
			wantSkip:  true,
			wantErr:   assert.NoError,
		},
		{
			name:      "bad_without_split_missing_separator",
			p:         NewProcessorEntity("test1"),
			arg:       "1234",
			wantEpoch: 0,
			wantSkip:  false,
			wantErr:   assert.Error,
		},
		{
			name:      "good_without_split",
			p:         NewProcessorEntity("test1"),
			arg:       "test1" + _defaultKeySeparator + "1234",
			wantEpoch: 1234,
			wantSkip:  false,
			wantErr:   assert.NoError,
		},
		{
			name:      "good_without_split_skip",
			p:         NewProcessorEntity("test1"),
			arg:       "test-not-this" + _defaultKeySeparator + "1234",
			wantEpoch: 1234,
			wantSkip:  true,
			wantErr:   assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.p
			gotEpoch, gotSkip, err := p.ParseOTWatcherKey(tt.arg)
			if !tt.wantErr(t, err, fmt.Sprintf("ParseOTWatcherKey(%v)", tt.arg)) {
				return
			}
			assert.Equalf(t, tt.wantEpoch, gotEpoch, "ParseOTWatcherKey(%v)", tt.arg)
			assert.Equalf(t, tt.wantSkip, gotSkip, "ParseOTWatcherKey(%v)", tt.arg)
		})
	}
}

func TestProcessorEntity_splitKey(t *testing.T) {
	tests := []struct {
		name    string
		arg     string
		p       *ProcessorEntity
		want    string
		want1   string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "bad_expected_name",
			p:       NewProcessorEntity("test1"),
			arg:     "1234",
			want:    "",
			want1:   "",
			wantErr: assert.Error,
		},
		{
			name:    "good_same_bucket",
			p:       NewProcessorEntity("test1"),
			arg:     "p1" + _defaultKeySeparator + "1234",
			want:    "p1",
			want1:   "1234",
			wantErr: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.p
			got, got1, err := p.splitKey(tt.arg)
			if !tt.wantErr(t, err, fmt.Sprintf("splitKey(%v)", tt.arg)) {
				return
			}
			assert.Equalf(t, tt.want, got, "splitKey(%v)", tt.arg)
			assert.Equalf(t, tt.want1, got1, "splitKey(%v)", tt.arg)
		})
	}
}
