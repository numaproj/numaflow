package initializer

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
	"github.com/numaproj/numaflow/pkg/shared/kvs/jetstream"
	"github.com/numaproj/numaflow/pkg/sideinputs/utils"
)

// Delete mountPath directory if it exists
func cleanup(mountPath string) {
	if utils.CheckFileExists(mountPath) {
		_ = os.RemoveAll(mountPath)
	}
}

// TestSideInputsValueUpdates tests that the side input initializer updates the side input files
// by reading from the side input bucket.
func TestSideInputsInitializer_Success(t *testing.T) {
	var (
		keyspace   = "sideInputTestWatch"
		sideInputs = []string{"TEST", "TEST2"}
		dataTest   = []string{"HELLO", "HELLO2"}
	)
	mountPath, err := os.MkdirTemp("", "side-input")
	assert.NoError(t, err)
	defer cleanup(mountPath)

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// connect to NATS
	nc := natstest.JetStreamClient(t, s)
	defer nc.Close()

	// create JetStream Context
	js, err := nc.JetStreamContext(nats.PublishAsyncMaxPending(256))
	assert.NoError(t, err)

	// create side input bucket
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace,
		Description:  fmt.Sprintf("[%s] side input bucket", keyspace),
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace) }()
	assert.NoError(t, err)

	bucketName := keyspace
	sideInputWatcher, _ := jetstream.NewKVJetStreamKVWatch(ctx, bucketName, nc)
	for x := range sideInputs {
		_, err = kv.Put(sideInputs[x], []byte(dataTest[x]))
		if err != nil {
			fmt.Println("Error in writing to bucket ", err)
		}
	}
	err = startSideInputInitializer(ctx, sideInputWatcher, mountPath, sideInputs)
	assert.NoError(t, err)

	for x, sideInput := range sideInputs {
		p := path.Join(mountPath, sideInput)
		fileData, err := utils.FetchSideInputFile(p)
		for err != nil {
			select {
			case <-ctx.Done():
				t.Fatalf("Context timeout")
			default:
				time.Sleep(10 * time.Millisecond)
				fileData, err = utils.FetchSideInputFile(p)
			}
		}
		assert.Equal(t, dataTest[x], string(fileData))
	}
}

// The startSideInputInitializer should wait until all the values are
// read from the KV bucket, thus this test should time out as we do not
// write any values to the store
func TestSideInputsTimeout(t *testing.T) {
	var (
		keyspace   = "sideInputTestWatch"
		sideInputs = []string{"TEST", "TEST2"}
	)
	mountPath, err := os.MkdirTemp("", "side-input")
	assert.NoError(t, err)
	defer cleanup(mountPath)

	// Create a new NATS server
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// connect to NATS
	nc := natstest.JetStreamClient(t, s)
	defer nc.Close()

	// create JetStream Context
	js, err := nc.JetStreamContext(nats.PublishAsyncMaxPending(256))
	assert.NoError(t, err)

	// create Side Input bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace,
		Description:  fmt.Sprintf("[%s] Side Input bucket", keyspace),
		MaxValueSize: 0,
		History:      0,
		TTL:          0,
		MaxBytes:     0,
		Storage:      nats.MemoryStorage,
		Replicas:     0,
		Placement:    nil,
	})
	defer func() { _ = js.DeleteKeyValue(keyspace) }()
	assert.NoError(t, err)

	bucketName := keyspace
	sideInputWatcher, _ := jetstream.NewKVJetStreamKVWatch(ctx, bucketName, nc)

	_ = startSideInputInitializer(ctx, sideInputWatcher, mountPath, sideInputs)
	assert.Equal(t, context.DeadlineExceeded, ctx.Err())
}
