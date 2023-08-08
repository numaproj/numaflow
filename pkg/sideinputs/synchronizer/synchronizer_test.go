package synchronizer

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
	"github.com/numaproj/numaflow/pkg/sideinputs/store/jetstream"
	"github.com/numaproj/numaflow/pkg/sideinputs/utils"
)

// Delete mountPath directory if it exists
func cleanup(mountPath string) {
	if utils.CheckFileExists(mountPath) {
		_ = os.RemoveAll(mountPath)
	}
}
func TestSideInputsValueUpdates(t *testing.T) {
	var (
		keyspace     = "sideInputTestWatch"
		pipelineName = "testPipeline"
		sideInputs   = []string{"TEST", "TEST2"}
		dataTest     = []string{"HELLO", "HELLO2"}
		mountPath    = "/tmp/side-input/"
	)
	defer cleanup(mountPath)

	// Remove any existing Side Input files
	for _, sideInput := range sideInputs {
		p := path.Join(mountPath, sideInput)
		os.Remove(p)
	}

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// connect to NATS
	nc := natstest.JetStreamClient(t, s)
	defer nc.Close()

	// create JetStream Context
	js, err := nc.JetStreamContext(nats.PublishAsyncMaxPending(256))
	assert.NoError(t, err)

	// create side-input bucket
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
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
	if utils.CheckFileExists(mountPath) == false {
		err = os.Mkdir(mountPath, 0777)
		assert.NoError(t, err)
	}

	bucketName := keyspace
	sideInputWatcher, _ := jetstream.NewKVJetStreamKVWatch(ctx, pipelineName, bucketName, nc)
	go startSideInputSynchronizer(ctx, sideInputWatcher, mountPath)
	for x := range sideInputs {
		_, err = kv.Put(sideInputs[x], []byte(dataTest[x]))
		if err != nil {
			assert.NoError(t, err)
		}
	}
	time.Sleep(1 * time.Second)
	for x, sideInput := range sideInputs {
		p := path.Join(mountPath, sideInput)
		fileData, err := utils.FetchSideInputFile(p)
		assert.NoError(t, err)
		assert.Equal(t, dataTest[x], string(fileData))
	}
}
