//go:build isb_redis

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

package isbsvc

import (
	"context"
	"testing"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
)

func TestIsbsRedisSvc_Buffers(t *testing.T) {
	ctx := context.Background()
	redisOptions := &goredis.UniversalOptions{
		Addrs: []string{":6379"},
	}
	buffer := "isbsRedisSvcBuffer"
	stream := redisclient.GetRedisStreamName(buffer)
	group := buffer + "-group"
	buffers := []dfv1.Buffer{{Name: buffer, Type: dfv1.EdgeBuffer}}
	redisClient := redisclient.NewRedisClient(redisOptions)
	isbsRedisSvc := NewISBRedisSvc(redisClient)
	assert.NoError(t, isbsRedisSvc.CreateBuffers(ctx, buffers))

	// validate buffer
	assert.NoError(t, isbsRedisSvc.ValidateBuffers(ctx, buffers))

	// Verify
	// Add some data
	startTime := time.Unix(1636470000, 0)
	messages := testutils.BuildTestWriteMessages(int64(10), startTime)
	// Add 10 messages
	for _, msg := range messages {
		err := redisClient.Client.XAdd(ctx, &goredis.XAddArgs{
			Stream: stream,
			Values: []interface{}{msg.Header, msg.Body},
		}).Err()
		assert.NoError(t, err)
	}

	// Read all the messages.
	rqr, _ := redis.NewBufferRead(ctx, redisClient, buffer, group, "consumer").(*redis.BufferRead)

	readMessages, err := rqr.Read(ctx, 10)
	assert.Nil(t, err)
	// ACK just 1 message, which leaves a pending count of 9
	var readOffsets = make([]string, 1)
	readOffsets[0] = readMessages[0].ReadOffset.String()
	_ = redisClient.Client.XAck(redisclient.RedisContext, stream, group, readOffsets...).Err()

	// test GetBufferInfo
	for _, buffer := range buffers {
		bufferInfo, err := isbsRedisSvc.GetBufferInfo(ctx, buffer)
		assert.NoError(t, err)
		assert.Equal(t, int64(9), bufferInfo.PendingCount)
	}

	// delete buffer
	assert.NoError(t, isbsRedisSvc.DeleteBuffers(ctx, buffers))
}
