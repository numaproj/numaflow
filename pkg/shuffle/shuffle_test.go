package shuffle

import (
	"fmt"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestShuffle_ShuffleMessages(t *testing.T) {
	bufferIdList := []string{
		"buffer-1",
		"buffer-2",
		"buffer-3",
		"buffer-4",
	}

	// create shuffle with buffer id list
	shuffler := NewShuffle(bufferIdList)

	// build test messages
	messages := testutils.BuildTestWriteMessages(10000, time.Now())
	// set key for messages
	var testMessages []*isb.Message
	for index := 0; index < len(messages); index++ {
		messages[index].Key = fmt.Sprintf("key_%d", index)
		testMessages = append(testMessages, &messages[index])
	}

	bufferIdMessageMap := shuffler.ShuffleMessages(testMessages)
	sum := 0
	for _, value := range bufferIdMessageMap {
		sum += len(value)
	}

	assert.Equal(t, sum, len(messages))
}
