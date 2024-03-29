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

package udsource

import (
	"testing"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"
	"github.com/stretchr/testify/assert"
)

func TestOffsetConversion(t *testing.T) {
	offset := &sourcepb.Offset{Offset: []byte("test"), PartitionId: 0}
	testIsbOffset := NewUserDefinedSourceOffset(offset)
	ConvertToUserDefinedSourceOffset(testIsbOffset)
	convertedBackIsbOffset := NewUserDefinedSourceOffset(offset)
	assert.Equal(t, testIsbOffset.PartitionIdx(), convertedBackIsbOffset.PartitionIdx())
	assert.Equal(t, testIsbOffset.String(), convertedBackIsbOffset.String())
	testSrcOffset := &sourcepb.Offset{
		PartitionId: 0,
		Offset:      []byte("test"),
	}
	convertedIsbOffset := NewUserDefinedSourceOffset(offset)
	convertedBackSrcOffset := ConvertToUserDefinedSourceOffset(convertedIsbOffset)
	assert.Equal(t, testSrcOffset.GetPartitionId(), convertedBackSrcOffset.GetPartitionId())
	assert.Equal(t, testSrcOffset.GetOffset(), convertedBackSrcOffset.GetOffset())
}
