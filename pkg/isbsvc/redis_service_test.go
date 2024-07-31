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

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/pkg/isb/stores/redis"

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
	buffers := []string{buffer}
	redisClient := redisclient.NewRedisClient(redisOptions)
	isbsRedisSvc := NewISBRedisSvc(redisClient)
	assert.NoError(t, isbsRedisSvc.CreateBuffersAndBuckets(ctx, buffers, nil, "", []string{}))

	// validate buffer
	assert.NoError(t, isbsRedisSvc.ValidateBuffersAndBuckets(ctx, buffers, nil, "", []string{}))

	// Verify
	// Add some data
	startTime := time.Unix(1636470000, 0)
	messages := testutils.BuildTestWriteMessages(int64(10), startTime, nil, "testVertex")
	// Add 10 messages
	for _, msg := range messages {
		err := redisClient.Client.XAdd(ctx, &goredis.XAddArgs{
			Stream: stream,
			Values: []interface{}{msg.Header, msg.Body.Payload},
		}).Err()
		assert.NoError(t, err)
	}

	// Read all the messages.
	rqr, _ := redis.NewBufferRead(ctx, redisClient, buffer, group, "consumer", 0).(*redis.BufferRead)

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
	assert.NoError(t, isbsRedisSvc.DeleteBuffersAndBuckets(ctx, buffers, nil, "", []string{}))
}
