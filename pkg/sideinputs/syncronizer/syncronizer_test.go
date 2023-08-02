package syncronizer

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sideinputs/store/jetstream"
	"github.com/numaproj/numaflow/pkg/sideinputs/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"sync"
	"testing"
	"time"
)

func TestSideInputsTimeout(t *testing.T) {
	var (
		keyspace     = "sideInputTestWatch"
		pipelineName = "testPipeline"
		mountPath    = "/tmp/side-input/"
	)
	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	wg := sync.WaitGroup{}
	wg.Add(1)
	go startSideInputSyncronizer(ctx, sideInputWatcher, log, mountPath, &wg)
	wg.Wait()
	assert.Equal(t, context.DeadlineExceeded, ctx.Err())
}

func TestSideInputsValueUpdates(t *testing.T) {
	var (
		keyspace     = "sideInputTestWatch"
		pipelineName = "testPipeline"
		sideInputs   = []string{"TEST", "TEST2"}
		dataTest     = []string{"HELLO", "HELLO2"}
		mountPath    = "/tmp/side-input/"
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
	if utils.CheckFileExists(mountPath) == false {
		err = os.Mkdir(mountPath, 0777)
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	bucketName := keyspace
	sideInputWatcher, _ := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, bucketName, nc)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go startSideInputSyncronizer(ctx, sideInputWatcher, log, mountPath, &wg)
	for x := range sideInputs {
		_, err = kv.Put(sideInputs[x], []byte(dataTest[x]))
		if err != nil {
			fmt.Println("ERROR ", err)
		}
	}
	wg.Wait()
	for x, sideInput := range sideInputs {
		p := path.Join(mountPath, sideInput)
		fileData, err := utils.FetchSideInputStore(p)
		assert.NoError(t, err)
		assert.Equal(t, dataTest[x], string(fileData))
	}
	assert.Equal(t, context.DeadlineExceeded, ctx.Err())
}
