//go:build isb_jetstream

package fetch

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
)

func TestRetryUntilSuccessfulWatcherCreation(t *testing.T) {
	// Connect to NATS
	nc, err := jsclient.NewDefaultJetStreamClient(nats.DefaultURL).Connect(context.TODO())
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

	watcher := RetryUntilSuccessfulWatcherCreation(js, "utilTest", false, zaptest.NewLogger(t).Sugar())
	assert.NotNil(t, watcher)

	watcherNil := RetryUntilSuccessfulWatcherCreation(js, "nonExist", false, zaptest.NewLogger(t).Sugar())
	assert.Nil(t, watcherNil)
}
