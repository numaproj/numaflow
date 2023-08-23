package utils

import (
	"testing"

	sourcepb "github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1"
	"github.com/stretchr/testify/assert"
)

func TestOffsetConversion(t *testing.T) {
	testIsbOffset := NewSimpleSourceOffset("test", 0)
	convertedSrcOffset := ConvertToSourceOffset(testIsbOffset)
	convertedBackIsbOffset := ConvertToIsbOffset(convertedSrcOffset)
	assert.Equal(t, testIsbOffset.PartitionIdx(), convertedBackIsbOffset.PartitionIdx())
	assert.Equal(t, testIsbOffset.String(), convertedBackIsbOffset.String())
	testSrcOffset := &sourcepb.Offset{
		PartitionId: "0",
		Offset:      []byte("test"),
	}
	convertedIsbOffset := ConvertToIsbOffset(testSrcOffset)
	convertedBackSrcOffset := ConvertToSourceOffset(convertedIsbOffset)
	assert.Equal(t, testSrcOffset.GetPartitionId(), convertedBackSrcOffset.GetPartitionId())
	assert.Equal(t, testSrcOffset.GetOffset(), convertedBackSrcOffset.GetOffset())
}
