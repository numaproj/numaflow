//go:build isb_jetstream

package fetch

import (
	"context"
	"testing"

	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/stretchr/testify/assert"
)

func TestFromProcessor_setStatus(t *testing.T) {
	var ctx = context.Background()
	p := NewProcessorToFetch(ctx, processor.NewProcessorEntity("testPod1", "test"), 5, nil)
	p.setStatus(_inactive)
	assert.Equal(t, _inactive, p.status)
}
