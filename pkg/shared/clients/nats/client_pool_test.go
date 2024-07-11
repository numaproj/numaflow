package nats

import (
	"context"
	"os"
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestNewClientPool_Success(t *testing.T) {
	os.Setenv(dfv1.EnvISBSvcJetStreamURL, "nats://localhost:4222")
	os.Setenv(dfv1.EnvISBSvcJetStreamUser, "user")
	os.Setenv(dfv1.EnvISBSvcJetStreamPassword, "password")
	ctx := context.Background()
	pool, err := NewClientPool(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, pool)
	assert.Equal(t, 3, pool.clients.Len()) // Check if the pool size matches the default clientPoolSize
}

func TestClientPool_NextAvailableClient(t *testing.T) {
	os.Setenv(dfv1.EnvISBSvcJetStreamURL, "nats://localhost:4222")
	os.Setenv(dfv1.EnvISBSvcJetStreamUser, "user")
	os.Setenv(dfv1.EnvISBSvcJetStreamPassword, "password")
	ctx := context.Background()
	pool, err := NewClientPool(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, pool)

	client1 := pool.NextAvailableClient()
	assert.NotNil(t, client1)

	client2 := pool.NextAvailableClient()
	assert.NotNil(t, client2)

	client3 := pool.NextAvailableClient()
	assert.NotNil(t, client3)
}
