//go:build isb_jetstream

package fetch

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/stretchr/testify/assert"
)

func TestRetryUntilSuccessfulWatcherCreation(t *testing.T) {
	var ctx = context.Background()
	// Connect to NATS
	nc, err := nats.Connect(nats.DefaultURL)
	assert.Nil(t, err)

	// Create JetStream Context
	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	assert.Nil(t, err)

	js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       "utilTest",
		Description:  "",
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer js.DeleteKeyValue("utilTest")

	watcher := RetryUntilSuccessfulWatcherCreation(js, "utilTest", false, logging.FromContext(ctx))
	assert.NotNil(t, watcher)

	watcherNil := RetryUntilSuccessfulWatcherCreation(js, "nonExist", false, logging.FromContext(ctx))
	assert.Nil(t, watcherNil)
}
