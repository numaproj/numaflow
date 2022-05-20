package publish

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestOptions(t *testing.T) {
	testOpts := []PublishOption{
		WithAutoRefreshHeartbeat(true),
		WithPodHeartbeatRate(10),
	}
	opts := &publishOptions{
		autoRefreshHeartbeat: false,
		podHeartbeatRate:     5,
	}
	for _, opt := range testOpts {
		opt(opts)
	}
	assert.True(t, opts.autoRefreshHeartbeat)
	assert.Equal(t, int64(10), opts.podHeartbeatRate)
}
