package isbsvc

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	nats2 "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
)

func TestJetstreamSvc_CreationDeletionValidation(t *testing.T) {
	ctx := context.Background()
	s := test.RunJetStreamServer(t)
	defer test.ShutdownJetStreamServer(t, s)

	conn, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	defer conn.Close()

	client := nats2.NewTestClient(t, s.ClientURL())
	defer client.Close()

	isbSvc, err := NewISBJetStreamSvc(client)
	assert.NoError(t, err)

	buffers := []string{"test-buffer-1", "test-buffer-2"}
	buckets := []string{"test-bucket-1", "test-bucket-2"}
	servingStreamStore := "test-serving-stream-1"
	sideInputStore := "test-side-input-store"

	err = isbSvc.CreateBuffersAndBuckets(ctx, buffers, buckets, sideInputStore, servingStreamStore)
	assert.NoError(t, err)

	err = isbSvc.ValidateBuffersAndBuckets(ctx, buffers, buckets, sideInputStore, servingStreamStore)
	assert.NoError(t, err)

	err = isbSvc.DeleteBuffersAndBuckets(ctx, buffers, buckets, sideInputStore, servingStreamStore)
	assert.NoError(t, err)
}

func TestJetstreamSvc_GetBufferInfo(t *testing.T) {
	ctx := context.Background()
	s := test.RunJetStreamServer(t)
	defer test.ShutdownJetStreamServer(t, s)

	conn, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	defer conn.Close()

	client := nats2.NewTestClient(t, s.ClientURL())
	defer client.Close()

	jsCtx, err := client.JetStreamContext()
	assert.NoError(t, err)

	// add stream
	_, err = jsCtx.AddStream(&nats.StreamConfig{
		Name: "test-buffer",
	})
	assert.NoError(t, err)

	// add consumer
	_, err = jsCtx.AddConsumer("test-buffer", &nats.ConsumerConfig{
		Name: "test-buffer",
	})
	assert.NoError(t, err)

	isbSvc, err := NewISBJetStreamSvc(client)
	assert.NoError(t, err)

	buffer := "test-buffer"
	info, err := isbSvc.GetBufferInfo(ctx, buffer)
	assert.NoError(t, err)
	assert.Equal(t, buffer, info.Name)
	assert.Equal(t, int64(0), info.PendingCount)
	assert.Equal(t, int64(0), info.AckPendingCount)
	assert.Equal(t, int64(0), info.TotalMessages)
}
