package nats

import (
	"context"
	"os"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	natstestserver "github.com/nats-io/nats-server/v2/test"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"

	// natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func TestNewNATSClient(t *testing.T) {
	// Setting up environment variables for the test
	os.Setenv(dfv1.EnvISBSvcJetStreamURL, "nats://localhost:4222")
	os.Setenv(dfv1.EnvISBSvcJetStreamUser, "user")
	os.Setenv(dfv1.EnvISBSvcJetStreamPassword, "password")
	defer os.Clearenv()

	log := zap.NewNop().Sugar()

	ctx := logging.WithLogger(context.Background(), log)

	client, err := NewNATSClient(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	// Cleanup
	client.Close()
}

func TestNewNATSClient_Failure(t *testing.T) {
	// Simulating environment variable absence
	os.Clearenv()

	log := zap.NewNop().Sugar()
	ctx := logging.WithLogger(context.Background(), log)

	client, err := NewNATSClient(ctx)
	assert.Error(t, err)
	assert.Nil(t, client)
}

func RunJetStreamServer(t *testing.T) *server.Server {
	t.Helper()
	opts := natstestserver.DefaultTestOptions
	opts.Port = -1 // Random port
	opts.JetStream = true
	storeDir, err := os.MkdirTemp("", "")
	if err != nil {
		t.Fatalf("Error creating a temp dir: %v", err)
	}
	opts.StoreDir = storeDir
	return natstestserver.RunServer(&opts)
}

func TestSubscribe(t *testing.T) {
	s := RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	defer client.Close()

	// Create a stream
	js, err := client.nc.JetStream()
	assert.NoError(t, err)
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST_STREAM",
		Subjects: []string{"test.subject"},
	})
	assert.NoError(t, err)

	// Subscribe to a subject
	sub, err := client.Subscribe("test.subject", "TEST_STREAM")
	assert.NoError(t, err)
	assert.NotNil(t, sub)

	// Test failure case: Invalid stream
	_, err = client.Subscribe("balh", "INVALID_STREAM")
	assert.Error(t, err)
}

func TestBindKVStore(t *testing.T) {
	s := RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	defer client.Close()

	// Create a KeyValue store
	js, err := client.nc.JetStream()
	assert.NoError(t, err)
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "KV_TEST",
	})
	assert.NoError(t, err)

	// Bind to the KeyValue store
	kvStore, err := client.BindKVStore("KV_TEST")
	assert.NoError(t, err)
	assert.NotNil(t, kvStore)

	// Test failure case: Invalid KeyValue store
	_, err = client.BindKVStore("INVALID_KV")
	assert.Error(t, err)
}

// func TestPendingForStream(t *testing.T) {
// 	s := RunJetStreamServer(t)
// 	defer s.Shutdown()

// 	client := NewTestClient(t, s.ClientURL())
// 	defer client.Close()

// 	// Create a stream and a consumer
// 	js, err := client.nc.JetStream()
// 	assert.NoError(t, err)
// 	_, err = js.AddStream(&nats.StreamConfig{
// 		Name:     "TEST_STREAM",
// 		Subjects: []string{"test.subject"},
// 	})
// 	assert.NoError(t, err)

// 	_, err = js.AddConsumer("TEST_STREAM", &nats.ConsumerConfig{
// 		Durable:   "TEST_CONSUMER",
// 		AckPolicy: nats.AckExplicitPolicy,
// 	})
// 	assert.NoError(t, err)

// 	// Publish messages to the stream
// 	for i := 0; i < 5; i++ {
// 		js.Publish("test.subject", []byte("message"))
// 	}

// 	// Check pending messages
// 	pending, err := client.PendingForStream("TEST_STREAM", "TEST_CONSUMER")
// 	assert.NoError(t, err)
// 	assert.Equal(t, int64(5), pending)

// 	// Test failure case: Invalid consumer
// 	_, err = client.PendingForStream("INVALID_CONSUMER", "TEST_STREAM")
// 	assert.Error(t, err)
// }

func TestJetStreamContext(t *testing.T) {
	s := RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	defer client.Close()

	jsCtx, err := client.JetStreamContext()
	assert.NoError(t, err)
	assert.NotNil(t, jsCtx)
}

func TestNewTestClient(t *testing.T) {
	s := RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	assert.NotNil(t, client)
	defer client.Close()
}

func TestClose(t *testing.T) {
	s := RunJetStreamServer(t)
	defer s.Shutdown()

	client := NewTestClient(t, s.ClientURL())
	assert.NotNil(t, client)
	client.Close()
}
