package initializer

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sideinputs/store/jetstream"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestSideInputsInitializer_Success(t *testing.T) {
	var (
		keyspace     = "sideInputTestWatch"
		wg           sync.WaitGroup
		pipelineName = "testPipeline"
		sideInputs   = []string{"TEST", "TEST2"}
		dataTest     = []string{"HELLO", "HELLO2"}
	)
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
	retCh := make(chan map[string][]byte, 1)
	m := createSideInputMap(sideInputs)
	wg.Add(1)
	go startSideInputWatcher(ctx, sideInputWatcher, &wg, retCh, log, m)
	expectedOut := make(map[string][]byte)
	for x := range sideInputs {
		_, err = kv.Put(sideInputs[x], []byte(dataTest[x]))
		if err != nil {
			fmt.Println("ERROR ", err)
		}
		expectedOut[sideInputs[x]] = []byte(dataTest[x])
	}
	wg.Wait()
	close(retCh)
	for x := range retCh {
		assert.Equal(t, x, expectedOut)
	}
}

func TestSideInputsTimeout(t *testing.T) {
	var (
		keyspace     = "sideInputTestWatch"
		wg           sync.WaitGroup
		pipelineName = "testPipeline"
		sideInputs   = []string{"TEST", "TEST2"}
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

	// create heartbeat bucket
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
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
	retCh := make(chan map[string][]byte, 1)
	m := createSideInputMap(sideInputs)
	wg.Add(1)
	go startSideInputWatcher(ctx, sideInputWatcher, &wg, retCh, log, m)
	wg.Wait()
	close(retCh)
	assert.Equal(t, context.DeadlineExceeded, ctx.Err())
}
