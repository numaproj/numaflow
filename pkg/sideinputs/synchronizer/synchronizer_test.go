/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/kvs/jetstream"
	"github.com/numaproj/numaflow/pkg/sideinputs/utils"
)

// Delete mountPath directory if it exists
func cleanup(mountPath string) {
	if utils.CheckFileExists(mountPath) {
		_ = os.RemoveAll(mountPath)
	}
}

// TestSideInputsInitializer_Success tests the side input synchronizer, which should update the
// side input store path with updated values from the side input bucket.
func TestSideInputsValueUpdates(t *testing.T) {
	var (
		keyspace   = "sideInputTestWatch"
		sideInputs = []string{"TEST", "TEST2"}
		dataTest   = []string{"HELLO", "HELLO2"}
	)
	mountPath, err := os.MkdirTemp("/tmp", "side-input")
	assert.NoError(t, err)
	// Clean up
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
	sideInputStore, _ := jetstream.NewKVJetStreamKVStore(ctx, bucketName, nc)
	go startSideInputSynchronizer(ctx, sideInputStore, sideInputs, mountPath)
	for x := range sideInputs {
		_, err = kv.Put(sideInputs[x], []byte(dataTest[x]))
		if err != nil {
			assert.NoError(t, err)
		}
	}

	for x, sideInput := range sideInputs {
		p := path.Join(mountPath, sideInput)
		fileData, err := utils.FetchSideInputFileValue(p)
		for err != nil {
			select {
			case <-ctx.Done():
				t.Fatalf("Context timeout")
			default:
				time.Sleep(10 * time.Millisecond)
				fileData, err = utils.FetchSideInputFileValue(p)
			}
		}
		assert.Equal(t, dataTest[x], string(fileData))
	}
}
