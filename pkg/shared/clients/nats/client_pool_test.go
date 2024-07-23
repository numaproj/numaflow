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

package nats

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

func TestNewClientPool_Success(t *testing.T) {
	os.Setenv(dfv1.EnvISBSvcJetStreamURL, "nats://localhost:4222")
	os.Setenv(dfv1.EnvISBSvcJetStreamUser, "user")
	os.Setenv(dfv1.EnvISBSvcJetStreamPassword, "password")
	ctx := context.Background()
	pool, err := NewClientPool(ctx)

	assert.NoError(t, err)
	assert.NotNil(t, pool)
	assert.Equal(t, 3, pool.clients.Len()) // Check if the pool size matches the default clientPoolSize
}

func TestClientPool_NextAvailableClient(t *testing.T) {
	os.Setenv(dfv1.EnvISBSvcJetStreamURL, "nats://localhost:4222")
	os.Setenv(dfv1.EnvISBSvcJetStreamUser, "user")
	os.Setenv(dfv1.EnvISBSvcJetStreamPassword, "password")
	ctx := context.Background()
	pool, err := NewClientPool(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, pool)

	client1 := pool.NextAvailableClient()
	assert.NotNil(t, client1)

	client2 := pool.NextAvailableClient()
	assert.NotNil(t, client2)

	client3 := pool.NextAvailableClient()
	assert.NotNil(t, client3)
}

func TestClientPool_CloseAll(t *testing.T) {
	os.Setenv(dfv1.EnvISBSvcJetStreamURL, "nats://localhost:4222")
	os.Setenv(dfv1.EnvISBSvcJetStreamUser, "user")
	os.Setenv(dfv1.EnvISBSvcJetStreamPassword, "password")
	ctx := context.Background()
	pool, err := NewClientPool(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, pool)

	for e := pool.clients.Front(); e != nil; e = e.Next() {
		client := e.Value.(*Client)
		assert.False(t, client.nc.IsClosed())
	}

	pool.CloseAll()
	for e := pool.clients.Front(); e != nil; e = e.Next() {
		client := e.Value.(*Client)
		assert.True(t, client.nc.IsClosed())
	}
}
