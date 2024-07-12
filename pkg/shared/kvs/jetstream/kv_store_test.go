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

package jetstream

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	natsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	natstest "github.com/numaproj/numaflow/pkg/shared/clients/nats/test"
	"github.com/numaproj/numaflow/pkg/shared/kvs"
)

func TestJetStreamKVStoreOperations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	kvName := "testJetStreamKVStore"

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	testClient := natsclient.NewTestClientWithServer(t, s)
	defer testClient.Close()

	js, err := testClient.JetStreamContext()
	assert.NoError(t, err)

	// create a kv bucket for testing
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: kvName,
	})
	assert.NoError(t, err)

	defer func() {
		err = js.DeleteKeyValue(kvName)
		assert.NoError(t, err)
	}()

	kvStore, err := NewKVJetStreamKVStore(ctx, kvName, testClient)
	assert.NoError(t, err)
	defer kvStore.Close()

	// Test Put
	err = kvStore.PutKV(ctx, "key1", []byte("value1"))
	assert.NoError(t, err)

	err = kvStore.PutKV(ctx, "key2", []byte("value2"))
	assert.NoError(t, err)

	// Test Get
	value, err := kvStore.GetValue(ctx, "key1")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value1"), value)

	value, err = kvStore.GetValue(ctx, "key2")
	assert.NoError(t, err)
	assert.Equal(t, []byte("value2"), value)

	// Test get all keys
	keys, err := kvStore.GetAllKeys(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"key1", "key2"}, keys)

	// Test delete
	err = kvStore.DeleteKey(ctx, "key1")
	assert.NoError(t, err)

	// Test get all keys
	keys, err = kvStore.GetAllKeys(ctx)
	assert.NoError(t, err)
	assert.Equal(t, []string{"key2"}, keys)
}

func TestJetStreamKVStoreWatch(t *testing.T) {
	kvName := "testJetStreamKVStore"

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	testClient := natsclient.NewTestClientWithServer(t, s)
	defer testClient.Close()

	js, err := testClient.JetStreamContext()
	assert.NoError(t, err)

	// create a kv bucket for testing
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: kvName,
	})
	assert.NoError(t, err)

	defer func() {
		err = js.DeleteKeyValue(kvName)
		assert.NoError(t, err)
	}()

	kvStore, err := NewKVJetStreamKVStore(ctx, kvName, testClient)
	assert.NoError(t, err)
	defer kvStore.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// write 100 key value pairs
		for i := 0; i < 100; i++ {
			err = kvStore.PutKV(ctx, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
			assert.NoError(t, err)
		}

		for i := 0; i < 50; i++ {
			err = kvStore.DeleteKey(ctx, fmt.Sprintf("key-%d", i))
		}
	}()

	// watch for the key value entries
	kvPutCount := 0
	kvDelCount := 0
	kvCh := kvStore.Watch(ctx)
watchLoop:
	for {
		select {
		case kve, ok := <-kvCh:
			if !ok {
				break watchLoop
			}
			if kve.Operation() == kvs.KVPut {
				kvPutCount++
			} else if kve.Operation() == kvs.KVDelete {
				kvDelCount++
			}
			if kvPutCount == 100 && kvDelCount == 50 {
				break watchLoop
			}
		case <-ctx.Done():
			assert.Fail(t, "context done")
			break watchLoop
		}
	}

	wg.Wait()
	assert.Equal(t, 100, kvPutCount)
	assert.Equal(t, 50, kvDelCount)
}

func TestJetStreamKVWithoutUpdates(t *testing.T) {
	kvName := "testJetStreamKVStore"

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	testClient := natsclient.NewTestClientWithServer(t, s)
	defer testClient.Close()

	js, err := testClient.JetStreamContext()
	assert.NoError(t, err)

	// create a kv bucket for testing
	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: kvName,
	})
	assert.NoError(t, err)

	defer func() {
		err = js.DeleteKeyValue(kvName)
		assert.NoError(t, err)
	}()

	kvStore, err := NewKVJetStreamKVStore(ctx, kvName, testClient, WithWatcherCreationThreshold(1*time.Second))
	assert.NoError(t, err)
	defer kvStore.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	// write some key value entries inside a go routine
	go func() {
		defer wg.Done()
		// write 50 key value pairs
		for i := 0; i < 50; i++ {
			err = kvStore.PutKV(ctx, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
			assert.NoError(t, err)
		}

		// don't write anything for 5 seconds
		time.Sleep(5 * time.Second)

		// write 50 more key value pairs
		for i := 50; i < 100; i++ {
			err = kvStore.PutKV(ctx, fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("value-%d", i)))
			assert.NoError(t, err)
		}
	}()

	// watch for the key value entries
	kvPutCount := 0

	kvCh := kvStore.Watch(ctx)
watchLoop:
	for {
		select {
		case kve, ok := <-kvCh:
			if !ok {
				break watchLoop
			}
			if kve.Operation() == kvs.KVPut {
				kvPutCount++
			}

			if kvPutCount == 100 {
				break watchLoop
			}
		case <-ctx.Done():
			assert.Fail(t, "context done")
			break watchLoop
		}
	}
	wg.Wait()
	assert.Equal(t, 100, kvPutCount)
}

func TestJetStreamKVStoreErrorBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kvName := "errorKVStore"

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	testClient := natsclient.NewTestClientWithServer(t, s)
	defer testClient.Close()

	// Intentionally binding to a non-existing bucket to simulate error
	_, err := NewKVJetStreamKVStore(ctx, kvName+"-non-existent", testClient)
	assert.Error(t, err)
}

func TestJetStreamKVStoreGetValueError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kvName := "testJetStreamKVStore"

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	testClient := natsclient.NewTestClientWithServer(t, s)
	defer testClient.Close()

	js, err := testClient.JetStreamContext()
	assert.NoError(t, err)

	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: kvName,
	})
	assert.NoError(t, err)

	defer func() {
		err = js.DeleteKeyValue(kvName)
		assert.NoError(t, err)
	}()

	kvStore, err := NewKVJetStreamKVStore(ctx, kvName, testClient)
	assert.NoError(t, err)
	defer kvStore.Close()

	// Attempt to get a non-existent key to simulate error
	_, err = kvStore.GetValue(ctx, "non-existent-key")
	assert.Error(t, err)
}

func TestJetStreamKVStoreListKeysError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kvName := "testJetStreamKVStore"

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	testClient := natsclient.NewTestClientWithServer(t, s)
	defer testClient.Close()

	js, err := testClient.JetStreamContext()
	assert.NoError(t, err)

	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: kvName,
	})
	assert.NoError(t, err)

	defer func() {
		err = js.DeleteKeyValue(kvName)
		assert.NoError(t, err)
	}()

	kvStore, err := NewKVJetStreamKVStore(ctx, kvName, testClient)
	assert.NoError(t, err)
	defer kvStore.Close()

	// Attempt to list keys in a non-existent bucket to simulate error
	_, err = NewKVJetStreamKVStore(ctx, "non-existent-bucket", testClient)
	assert.Error(t, err)
}

func TestJetStreamKVStorePutKVError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kvName := "testJetStreamKVStore"

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	testClient := natsclient.NewTestClientWithServer(t, s)
	defer testClient.Close()

	js, err := testClient.JetStreamContext()
	assert.NoError(t, err)

	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: kvName,
	})
	assert.NoError(t, err)

	defer func() {
		err = js.DeleteKeyValue(kvName)
		assert.NoError(t, err)
	}()

	kvStore, err := NewKVJetStreamKVStore(ctx, kvName, testClient)
	assert.NoError(t, err)
	defer kvStore.Close()

	// Forcing a Put error by using a bad key name
	err = kvStore.PutKV(ctx, "", []byte("value"))
	assert.Error(t, err)
}

func TestJetStreamKVStoreWatchError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	kvName := "testJetStreamKVStore"

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	testClient := natsclient.NewTestClientWithServer(t, s)
	defer testClient.Close()

	js, err := testClient.JetStreamContext()
	assert.NoError(t, err)

	_, err = js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: kvName,
	})
	assert.NoError(t, err)

	defer func() {
		err = js.DeleteKeyValue(kvName)
		assert.NoError(t, err)
	}()

	kvStore, err := NewKVJetStreamKVStore(ctx, kvName, testClient)
	assert.NoError(t, err)
	defer kvStore.Close()

	// Forcing a watch error by canceling the context immediately
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	cancel()

	kvCh := kvStore.Watch(ctx)
	_, ok := <-kvCh
	assert.False(t, ok)
}
func TestJetStreamKVStoreWithContextDone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()

	kvName := "testJetStreamKVStore"

	s := natstest.RunJetStreamServer(t)
	defer natstest.ShutdownJetStreamServer(t, s)

	testClient := natsclient.NewTestClientWithServer(t, s)
	defer testClient.Close()

	kvStore, err := NewKVJetStreamKVStore(ctx, kvName, testClient)
	assert.Error(t, err)
	assert.Nil(t, kvStore)
}
