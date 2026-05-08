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
		Name:     "test-buffer",
		Subjects: []string{"test-buffer"},
	})
	assert.NoError(t, err)

	// add consumer
	_, err = jsCtx.AddConsumer("test-buffer", &nats.ConsumerConfig{
		Durable:       "test-buffer",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "test-buffer",
	})
	assert.NoError(t, err)

	isbSvc, err := NewISBJetStreamSvc(client)
	assert.NoError(t, err)

	buffer := "test-buffer"

	// Empty-state baseline.
	info, err := isbSvc.GetBufferInfo(ctx, buffer)
	assert.NoError(t, err)
	assert.Equal(t, buffer, info.Name)
	assert.Equal(t, int64(0), info.PendingCount)
	assert.Equal(t, int64(0), info.AckPendingCount)
	assert.Equal(t, int64(0), info.TotalMessages)
	assert.Equal(t, int64(0), info.StreamMsgs)
	assert.Equal(t, int64(0), info.StreamFirstSeq)
	assert.Equal(t, int64(0), info.StreamLastSeq)
	assert.Equal(t, int64(0), info.StreamBytes)
	assert.Equal(t, int64(0), info.ConsumerNumRedelivered)
	assert.Equal(t, int64(0), info.ConsumerNumWaiting)
	assert.Equal(t, int64(0), info.ConsumerDeliveredStreamSeq)
	assert.Equal(t, int64(0), info.ConsumerAckFloorStreamSeq)

	// Publish 3 messages so stream state has content.
	for i := 0; i < 3; i++ {
		_, err = jsCtx.Publish("test-buffer", []byte("hello"))
		assert.NoError(t, err)
	}

	info, err = isbSvc.GetBufferInfo(ctx, buffer)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), info.StreamMsgs)
	assert.Equal(t, int64(1), info.StreamFirstSeq)
	assert.Equal(t, int64(3), info.StreamLastSeq)
	assert.Greater(t, info.StreamBytes, int64(0))
	// Nothing has been delivered/acked yet.
	assert.Equal(t, int64(3), info.PendingCount)
	assert.Equal(t, int64(0), info.AckPendingCount)
	assert.Equal(t, int64(0), info.ConsumerDeliveredStreamSeq)
	assert.Equal(t, int64(0), info.ConsumerAckFloorStreamSeq)
	assert.Equal(t, int64(0), info.ConsumerNumRedelivered)

	// Stream config snapshot — the test stream was created with default
	// nats.StreamConfig (zero-valued Retention/Discard), which the NATS
	// server interprets as LimitsPolicy + DiscardOld. The exact integer
	// values for unlimited fields are server-defined (typically 0 or -1
	// returned as-is); only assert that the strings render correctly.
	assert.Equal(t, "limits", info.StreamRetention)
	assert.Equal(t, "old", info.StreamDiscard)
	// Defaults are unlimited; the server may report 0 or -1. Either way
	// the field must be populated (i.e. parseable as int64) — accept both.
	assert.True(t, info.StreamMaxMsgs == 0 || info.StreamMaxMsgs == -1,
		"unexpected StreamMaxMsgs: %d", info.StreamMaxMsgs)
	assert.True(t, info.StreamMaxBytes == 0 || info.StreamMaxBytes == -1,
		"unexpected StreamMaxBytes: %d", info.StreamMaxBytes)
	assert.GreaterOrEqual(t, info.StreamMaxAgeSec, int64(0))
	assert.GreaterOrEqual(t, info.StreamDuplicatesSec, int64(0))
}

func TestRetentionAndDiscardPolicyStringHelpers(t *testing.T) {
	assert.Equal(t, "limits", retentionPolicyString(nats.LimitsPolicy))
	assert.Equal(t, "interest", retentionPolicyString(nats.InterestPolicy))
	assert.Equal(t, "workqueue", retentionPolicyString(nats.WorkQueuePolicy))
	assert.Equal(t, "unknown", retentionPolicyString(nats.RetentionPolicy(99)))

	assert.Equal(t, "old", discardPolicyString(nats.DiscardOld))
	assert.Equal(t, "new", discardPolicyString(nats.DiscardNew))
	assert.Equal(t, "unknown", discardPolicyString(nats.DiscardPolicy(99)))
}
