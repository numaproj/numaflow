package publish

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	testOpts := []PublishOption{
		WithAutoRefreshHeartbeatDisabled(),
		WithPodHeartbeatRate(10),
	}
	opts := &publishOptions{
		autoRefreshHeartbeat: true,
		podHeartbeatRate:     5,
	}
	for _, opt := range testOpts {
		opt(opts)
	}
	assert.False(t, opts.autoRefreshHeartbeat)
	assert.Equal(t, int64(10), opts.podHeartbeatRate)
}
