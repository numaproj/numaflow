package tracker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
)

func TestTracker_AddRequest(t *testing.T) {
	readMessages := testutils.BuildTestReadMessages(3, time.Unix(1661169600, 0), nil)
	messages := make([]*isb.ReadMessage, len(readMessages))
	for i, msg := range readMessages {
		messages[i] = &msg
	}
	tr := NewMessageTracker(messages)
	id := readMessages[0].ReadOffset.String()
	m := tr.Remove(id)
	assert.NotNil(t, m)
	assert.Equal(t, readMessages[0], *m)
}

func TestTracker_RemoveRequest(t *testing.T) {
	readMessages := testutils.BuildTestReadMessages(3, time.Unix(1661169600, 0), nil)
	messages := make([]*isb.ReadMessage, len(readMessages))
	for i, msg := range readMessages {
		messages[i] = &msg
	}
	tr := NewMessageTracker(messages)
	id := readMessages[0].ReadOffset.String()
	m := tr.Remove(id)
	assert.NotNil(t, m)
	assert.Equal(t, readMessages[0], *m)
	m = tr.Remove(id)
	assert.Nil(t, m)
}
