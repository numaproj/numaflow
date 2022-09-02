package cat

import (
	"context"
	"testing"
	"time"

	funcsdk "github.com/numaproj/numaflow-go/function"
	"github.com/stretchr/testify/assert"
)

type testDatum struct {
	value     []byte
	eventTime time.Time
	watermark time.Time
}

func (h *testDatum) Value() []byte {
	return h.value
}

func (h *testDatum) EventTime() time.Time {
	return h.eventTime
}

func (h *testDatum) Watermark() time.Time {
	return h.watermark
}

func TestNew(t *testing.T) {
	ctx := context.Background()
	p := New()
	req := []byte{0}
	messages := p(ctx, "", &testDatum{
		value:     req,
		eventTime: time.Time{},
		watermark: time.Time{},
	})
	assert.Equal(t, 1, len(messages))
	assert.Equal(t, funcsdk.ALL, messages[0].Key)
	assert.Equal(t, req, messages[0].Value)
}
