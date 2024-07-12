package nats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultOptions(t *testing.T) {
	opts := defaultOptions()
	assert.NotNil(t, opts)
	assert.Equal(t, 3, opts.clientPoolSize, "default client pool size should be 3")
}

func TestWithClientPoolSize(t *testing.T) {
	opts := defaultOptions()
	assert.Equal(t, 3, opts.clientPoolSize, "default client pool size should be 3")

	option := WithClientPoolSize(10)
	option(opts)

	assert.Equal(t, 10, opts.clientPoolSize, "client pool size should be set to 10")
}

func TestCombinedOptions(t *testing.T) {
	opts := defaultOptions()
	assert.Equal(t, 3, opts.clientPoolSize, "default client pool size should be 3")

	option1 := WithClientPoolSize(5)
	option1(opts)

	assert.Equal(t, 5, opts.clientPoolSize, "client pool size should be set to 5")
}
