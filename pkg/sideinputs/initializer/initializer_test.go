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

func TestSideInputsInitializer_Run(t *testing.T) {
	var (
		keyspace     = "sideInputTestWatch"
		wg           sync.WaitGroup
		pipelineName = "testPipeline"
	)
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
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
	fmt.Println("BUCKET", kv.Bucket())

	bucketName := keyspace
	sideInputWatcher, _ := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, bucketName, nc)
	fmt.Println("KV", sideInputWatcher.GetKVName())
	dataTest := []byte("HELLO")
	_, err = kv.Put("TEST", dataTest)
	if err != nil {
		fmt.Println("ERROR ", err)
	}
	val, _ := kv.Get("TEST")
	fmt.Println("CHECK", string(val.Value()))
	//
	retCh := make(chan []byte, 1)
	wg.Add(1)
	go startSideInputWatcher(ctx, sideInputWatcher, &wg, retCh, log)
	wg.Wait()
	close(retCh)
}
