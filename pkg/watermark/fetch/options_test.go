package fetch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	testOpts := []VertexOption{
		WithPodHeartbeatRate(10),
		WithRefreshingProcessorsRate(15),
	}
	opts := &vertexOptions{
		podHeartbeatRate:         5,
		refreshingProcessorsRate: 5,
	}
	for _, opt := range testOpts {
		opt(opts)
	}
	assert.Equal(t, int64(10), opts.podHeartbeatRate)
	assert.Equal(t, int64(15), opts.refreshingProcessorsRate)
}
