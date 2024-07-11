package redis

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithoutPipelining(t *testing.T) {
	opts := &Options{}
	option := WithoutPipelining()
	option.Apply(opts)
	assert.False(t, opts.Pipelining)
}

func TestWithInfoRefreshInterval(t *testing.T) {
	opts := &Options{}
	interval := 10 * time.Second
	option := WithInfoRefreshInterval(interval)
	option.Apply(opts)
	assert.Equal(t, interval, opts.InfoRefreshInterval)
}

func TestWithLagDuration(t *testing.T) {
	opts := &Options{}
	lag := 5 * time.Minute
	option := WithLagDuration(lag)
	option.Apply(opts)
	assert.Equal(t, lag, opts.LagDuration)
}

func TestWithReadTimeOut(t *testing.T) {
	opts := &Options{}
	timeout := 2 * time.Second
	option := WithReadTimeOut(timeout)
	option.Apply(opts)
	assert.Equal(t, timeout, opts.ReadTimeOut)
}

func TestWithCheckBacklog(t *testing.T) {
	opts := &Options{}
	option := WithCheckBacklog(true)
	option.Apply(opts)
	assert.True(t, opts.CheckBackLog)
}

func TestWithMaxLength(t *testing.T) {
	opts := &Options{}
	maxLength := int64(100)
	option := WithMaxLength(maxLength)
	option.Apply(opts)
	assert.Equal(t, maxLength, opts.MaxLength)
}

func TestWithBufferUsageLimit(t *testing.T) {
	opts := &Options{}
	limit := float64(0.75)
	option := WithBufferUsageLimit(limit)
	option.Apply(opts)
	assert.Equal(t, limit, opts.BufferUsageLimit)
}

func TestWithRefreshBufferWriteInfo(t *testing.T) {
	opts := &Options{}
	option := WithRefreshBufferWriteInfo(true)
	option.Apply(opts)
	assert.True(t, opts.RefreshBufferWriteInfo)
}

func TestOptionInterfaceImplementation(t *testing.T) {
	var _ Option = pipelining(true)
	var _ Option = infoRefreshInterval(1 * time.Second)
	var _ Option = lagDuration(1 * time.Second)
	var _ Option = readTimeOut(1 * time.Second)
	var _ Option = checkBackLog(true)
	var _ Option = maxLength(100)
	var _ Option = bufferUsageLimit(0.5)
	var _ Option = refreshBufferWriteInfo(true)
}

// Test default values
func TestOptionsDefaultValues(t *testing.T) {
	opts := Options{}
	assert.False(t, opts.Pipelining)
	assert.Equal(t, time.Duration(0), opts.InfoRefreshInterval)
	assert.Equal(t, time.Duration(0), opts.LagDuration)
	assert.Equal(t, time.Duration(0), opts.ReadTimeOut)
	assert.False(t, opts.CheckBackLog)
	assert.Equal(t, int64(0), opts.MaxLength)
	assert.Equal(t, float64(0), opts.BufferUsageLimit)
	assert.False(t, opts.RefreshBufferWriteInfo)
}
