package rpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb/testutils"
)

func TestTracker_AddRequest(t *testing.T) {
	tr := NewTracker()
	readMessages := testutils.BuildTestReadMessages(3, time.Unix(1661169600, 0), nil)
	for _, msg := range readMessages {
		tr.addRequest(&msg)
	}
	id := readMessages[0].ReadOffset.String()
	m, ok := tr.getRequest(id)
	assert.True(t, ok)
	assert.Equal(t, readMessages[0], *m)
}

func TestTracker_RemoveRequest(t *testing.T) {
	tr := NewTracker()
	readMessages := testutils.BuildTestReadMessages(3, time.Unix(1661169600, 0), nil)
	for _, msg := range readMessages {
		tr.addRequest(&msg)
	}
	id := readMessages[0].ReadOffset.String()
	m, ok := tr.getRequest(id)
	assert.True(t, ok)
	assert.Equal(t, readMessages[0], *m)
	tr.removeRequest(id)
	_, ok = tr.getRequest(id)
	assert.False(t, ok)
}

func TestTracker_Clear(t *testing.T) {
	tr := NewTracker()
	readMessages := testutils.BuildTestReadMessages(3, time.Unix(1661169600, 0), nil)
	for _, msg := range readMessages {
		tr.addRequest(&msg)
	}
	tr.clear()
	id := readMessages[0].ReadOffset.String()
	_, ok := tr.getRequest(id)
	assert.False(t, ok)
}
