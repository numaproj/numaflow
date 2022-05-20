package cat

import (
	"context"
	"testing"

	funcsdk "github.com/numaproj/numaflow/sdks/golang/function"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	ctx := context.Background()
	p := New()
	req := []byte{0}
	messages, err := p(ctx, []byte(""), req)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(messages))
	assert.Equal(t, funcsdk.ALL, string(messages[0].Key))
	assert.Equal(t, req, messages[0].Value)
}
