package clients

import (
	"context"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestNewRedisClient(t *testing.T) {
	// TODO: fix the test
	t.SkipNow()
	ctx := context.TODO()
	client := NewRedisClient(&redis.UniversalOptions{
		Addrs: []string{":6379"},
	})
	var stream = "foo"
	var streamGroup = "foo-group"
	err := client.CreateStreamGroup(ctx, stream, streamGroup, ReadFromEarliest)
	assert.NoError(t, err)
	defer func() {
		err := client.DeleteStreamGroup(ctx, stream, streamGroup)
		assert.NoError(t, err)
	}()

	err = client.CreateStreamGroup(ctx, stream, streamGroup, ReadFromEarliest)
	assert.Error(t, err)
}
