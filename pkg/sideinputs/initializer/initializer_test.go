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
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sideinputs/store/jetstream"
	"github.com/numaproj/numaflow/pkg/sideinputs/utils"
)

func TestSideInputsInitializer_Success(t *testing.T) {
	var (
		keyspace     = "sideInputTestWatch"
		pipelineName = "testPipeline"
		sideInputs   = []string{"TEST", "TEST2"}
		dataTest     = []string{"HELLO", "HELLO2"}
		mountPath    = "/tmp/side-input/"
	)
	// Remove any existing side-input files
	for _, sideInput := range sideInputs {
		p := path.Join(mountPath, sideInput)
		os.Remove(p)
	}

	if utils.CheckFileExists(mountPath) == false {
		err := os.Mkdir(mountPath, 0777)
		assert.NoError(t, err)
	}

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	log := logging.FromContext(ctx)

	// connect to NATS
	nc := natstest.JetStreamClient(t, s)
	defer nc.Close()

	// create JetStream Context
	js, err := nc.JetStreamContext(nats.PublishAsyncMaxPending(256))
	assert.NoError(t, err)

	// create heartbeat bucket
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace,
		Description:  fmt.Sprintf("[%s] heartbeat bucket", keyspace),
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
	sideInputWatcher, _ := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, bucketName, nc)
	for x := range sideInputs {
		_, err = kv.Put(sideInputs[x], []byte(dataTest[x]))
		if err != nil {
			fmt.Println("Error in writing to bucket ", err)
		}
	}
	err = startSideInputInitializer(ctx, sideInputWatcher, log, mountPath, sideInputs)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)
	for x, sideInput := range sideInputs {
		p := path.Join(mountPath, sideInput)
		fileData, err := utils.FetchSideInputStore(p)
		assert.NoError(t, err)
		assert.Equal(t, dataTest[x], string(fileData))
	}
}

// The startSideInputInitializer should wait until all the values are
// read from the KV bucket, thus this test should time out as we do not
// write any values to the store
func TestSideInputsTimeout(t *testing.T) {
	var (
		keyspace     = "sideInputTestWatch"
		pipelineName = "testPipeline"
		sideInputs   = []string{"TEST", "TEST2"}
		mountPath    = "/tmp/side-input/"
	)
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log := logging.FromContext(ctx)

	// connect to NATS
	nc := natstest.JetStreamClient(t, s)
	defer nc.Close()

	// create JetStream Context
	js, err := nc.JetStreamContext(nats.PublishAsyncMaxPending(256))
	assert.NoError(t, err)

	// create side-input bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:       keyspace,
		Description:  fmt.Sprintf("[%s] side-input bucket", keyspace),
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
	sideInputWatcher, _ := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, bucketName, nc)

	_ = startSideInputInitializer(ctx, sideInputWatcher, log, mountPath, sideInputs)
	assert.Equal(t, context.DeadlineExceeded, ctx.Err())
}
