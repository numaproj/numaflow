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

package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/generic"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
)

func TestRedisQWrite_Write(t *testing.T) {
	client := redisclient.NewRedisClient(redisOptions)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancel()
	rqw, _ := NewBufferWrite(ctx, client, "rediswrite", "test", WithLagDuration(2*time.Millisecond), WithInfoRefreshInterval(2*time.Millisecond)).(*BufferWrite)

	for rqw.IsFull() {
		select {
		case <-ctx.Done():
			t.Fatalf("full, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	streamName := rqw.GetStreamName()
	defer func() { _ = client.DeleteKeys(ctx, streamName) }()
	hash1 := rqw.GetHashKeyName(testStartTime)
	defer func() { _ = client.DeleteKeys(ctx, hash1) }()
	hash2 := rqw.GetHashKeyName(time.Unix(1636470000+300, 0).UTC())
	defer func() { _ = client.DeleteKeys(ctx, hash2) }()
	// remove the script
	err := client.Client.ScriptFlush(ctx).Err()
	assert.NoError(t, err, "ScriptFlush failed")

	startTime := time.Unix(1636470000, 0)
	writeMessages, internalKeys := buildTestWriteMessages(rqw, int64(10), startTime)
	defer func() { _ = client.DeleteKeys(ctx, internalKeys...) }()
	_, errs := rqw.Write(ctx, writeMessages)
	assert.Equal(t, make([]error, len(writeMessages)), errs, "Write failed")
	writeMessages, internalKeys = buildTestWriteMessages(rqw, int64(10), startTime)
	defer func() { _ = client.DeleteKeys(ctx, internalKeys...) }()
	_, errs = rqw.Write(ctx, writeMessages)

	// we inserted 20, 10 of these are duplicates, but we should see only 10 in the queue

	// make sure we saw 10 elements in xstream
	result, err := client.Client.XLen(ctx, streamName).Result()
	assert.NoError(t, err, "expected no error")
	assert.Equal(t, result, int64(10), "there should be 10 elements in Q")

	result, err = client.Client.HLen(ctx, hash1).Result()
	assert.NoError(t, err, "expected no error")
	assert.Equal(t, result, int64(5), "there should be 5 elements in Hash")

	result, err = client.Client.HLen(ctx, hash2).Result()
	assert.NoError(t, err, "expected no error")
	assert.Equal(t, result, int64(5), "there should be 5 elements in Hash")
}

func TestRedisQWrite_WithPipeline(t *testing.T) {
	client := redisclient.NewRedisClient(redisOptions)
	ctx := context.Background()
	stream := "withPipeline"
	count := int64(100)
	rqw, _ := NewBufferWrite(ctx, client, stream, "test", WithoutPipelining()).(*BufferWrite)
	streamName := rqw.GetStreamName()
	defer func() { _ = client.DeleteKeys(ctx, streamName) }()
	group := "withPipeline-group"
	consumer := "withPipeline-0"

	rqr, _ := NewBufferRead(ctx, client, stream, group, consumer).(*BufferRead)
	err := client.CreateStreamGroup(ctx, rqr.GetStreamName(), group, redisclient.ReadFromEarliest)
	assert.NoError(t, err)
	defer func() { _ = client.DeleteStreamGroup(ctx, rqr.GetStreamName(), group) }()

	// remove the script
	err = client.Client.ScriptFlush(ctx).Err()
	assert.NoError(t, err, "ScriptFlush failed")

	writeMessages, internalKeys := buildTestWriteMessages(rqw, count, testStartTime)
	defer func() { _ = client.DeleteKeys(ctx, internalKeys...) }()
	_, errs := rqw.Write(ctx, writeMessages)
	assert.Equal(t, make([]error, len(writeMessages)), errs, "Write failed")
}

func TestRedisQWrite_WithoutPipeline(t *testing.T) {
	client := redisclient.NewRedisClient(redisOptions)
	ctx := context.Background()
	stream := "withoutPipeline"
	count := int64(100)
	rqw, _ := NewBufferWrite(ctx, client, stream, "test").(*BufferWrite)

	streamName := rqw.GetStreamName()
	defer func() { _ = client.DeleteKeys(ctx, streamName) }()
	group := "withoutPipeline-group"
	consumer := "withoutPipeline-0"

	rqr, _ := NewBufferRead(ctx, client, stream, group, consumer).(*BufferRead)
	err := client.CreateStreamGroup(ctx, rqr.GetStreamName(), group, redisclient.ReadFromEarliest)
	assert.NoError(t, err)

	defer func() { _ = client.DeleteStreamGroup(ctx, rqr.GetStreamName(), group) }()

	startTime := time.Unix(1636470000, 0)
	writeMessages, internalKeys := buildTestWriteMessages(rqw, count, startTime)
	defer func() { _ = client.DeleteKeys(ctx, internalKeys...) }()
	_, errs := rqw.Write(ctx, writeMessages)
	assert.Equal(t, make([]error, len(writeMessages)), errs, "Write failed")
}

func TestRedisQWrite_WithInfoRefreshInterval(t *testing.T) {
	client := redisclient.NewRedisClient(redisOptions)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	stream := "withInfoRefreshInterval"
	count := int64(10)
	group := "withInfoRefreshInterval-group"
	rqw, _ := NewBufferWrite(ctx, client, stream, group, WithInfoRefreshInterval(2*time.Millisecond), WithLagDuration(2*time.Millisecond), WithMaxLength(10)).(*BufferWrite)
	err := client.CreateStreamGroup(ctx, rqw.GetStreamName(), group, redisclient.ReadFromEarliest)
	if err != nil {
		t.Fatalf("error creating consumer group: %s", err)
	}
	defer func() { _ = client.DeleteStreamGroup(ctx, rqw.GetStreamName(), group) }()
	defer func() { _ = client.DeleteKeys(ctx, rqw.GetStreamName()) }()

	writeMessages, internalKeys := buildTestWriteMessages(rqw, count, testStartTime)
	defer func() { _ = client.DeleteKeys(ctx, internalKeys...) }()
	_, errs := rqw.Write(ctx, writeMessages)
	assert.Equal(t, make([]error, len(writeMessages)), errs, "Write failed")

	for !rqw.IsFull() {
		select {
		case <-ctx.Done():
			t.Fatalf("full, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	// Once full try to write messages and that
	writeMessages, internalKeys = buildTestWriteMessages(rqw, count, testStartTime)
	defer func() { _ = client.DeleteKeys(ctx, internalKeys...) }()
	_, errs = rqw.Write(ctx, writeMessages)

	// assert the actual error that the buffer is full
	for _, err := range errs {
		assert.Equal(t, err, isb.BufferWriteErr{Name: stream, Full: true, Message: "Buffer full!"})
	}
}

// buildTestWriteMessages a list test messages and the internal hashKeys it created
func buildTestWriteMessages(rqw *BufferWrite, count int64, startTime time.Time) ([]isb.Message, []string) {
	var messages = make([]isb.Message, 0, count)
	var internalHashKeysMap map[string]bool
	var internalHashKeys = make([]string, 0)
	messages = append(messages, testutils.BuildTestWriteMessages(count, startTime)...)
	for i := int64(0); i < count; i++ {
		tmpTime := startTime.Add(time.Duration(i) * time.Minute)
		hashKeyName := rqw.GetHashKeyName(tmpTime)
		if ok := internalHashKeysMap[hashKeyName]; !ok {
			internalHashKeys = append(internalHashKeys, hashKeyName)
		}
	}

	return messages, internalHashKeys
}

func TestLua(t *testing.T) {
	ctx := context.Background()
	client := redis.NewUniversalClient(redisOptions)
	message := isb.Message{Header: isb.Header{ID: "0", PaneInfo: isb.PaneInfo{EventTime: testStartTime}}, Body: isb.Body{Payload: []byte("foo")}}
	script := redis.NewScript(exactlyOnceInsertLuaScript)

	var hashName = "{step-1}:1234567890:hash-foo"
	var streamName = "{step-1}:stream-foo"
	// cleanup afterwards
	defer func() { client.Del(ctx, hashName, streamName) }()

	// first insert
	res, err := script.Run(ctx, client, []string{hashName, streamName}, "10", message.Header, message.Payload, "0-0").Result()
	assert.NoErrorf(t, err, "lua script execution failed, %s", err)
	id, err := splitId(res.(string))
	assert.NoError(t, err)
	assert.Positive(t, id, res)

	// duplicate insert
	res, err = script.Run(ctx, client, []string{hashName, streamName}, "10", message.Header, message.Payload, "0-0").Result()
	assert.NoErrorf(t, err, "lua script execution failed, %s", err)
	id, err = splitId(res.(string))
	assert.NoError(t, err)
	assert.Positive(t, id, res)
}

func Test_initializeErrorArray(t *testing.T) {
	count := 10
	var errs = make([]error, count)
	var err = fmt.Errorf("test error")
	initializeErrorArray(errs, err)
	var expectedErrs = make([]error, count)
	for i := range expectedErrs {
		expectedErrs[i] = err
	}
	assert.Equal(t, expectedErrs, errs)
}

// Test_updateIsFullFlag tests by writing a bunch of messages and once the buffer is full it throws an error saying
// buffer is full
func Test_updateIsFullFlag(t *testing.T) {

	client := redisclient.NewRedisClient(redisOptions)
	ctx := context.Background()
	stream := "getConsumerLag"
	group := "getConsumerLag-group"
	count := int64(9)

	rqw, _ := NewBufferWrite(ctx, client, stream, group, WithInfoRefreshInterval(2*time.Millisecond), WithMaxLength(10)).(*BufferWrite)
	err := client.CreateStreamGroup(ctx, rqw.GetStreamName(), group, redisclient.ReadFromEarliest)
	defer func() { _ = client.DeleteStreamGroup(ctx, rqw.GetStreamName(), group) }()

	streamName := rqw.GetStreamName()
	defer func() { _ = client.DeleteKeys(ctx, streamName) }()

	startTime := time.Unix(1636470000, 0)
	writeMessages, internalKeys := buildTestWriteMessages(rqw, count, startTime)
	defer func() { _ = client.DeleteKeys(ctx, internalKeys...) }()
	_, errs := rqw.Write(ctx, writeMessages)

	// assert there are no errors
	result, err := client.Client.XLen(ctx, streamName).Result()
	assert.NoError(t, err, "expected no error")
	assert.Equal(t, result, int64(9), "there should be 10 elements in Q")

	rqw.updateIsFullAndLag(ctx)

	writeMessages, internalKeys = buildTestWriteMessages(rqw, count, startTime)
	defer func() { _ = client.DeleteKeys(ctx, internalKeys...) }()
	_, errs = rqw.Write(ctx, writeMessages)

	// assert the actual error that the buffer is full
	for _, err := range errs {
		assert.Equal(t, err, isb.BufferWriteErr{Name: stream, Full: true, Message: "Buffer full!"})
	}
}

func Test_GetName(t *testing.T) {
	client := redisclient.NewRedisClient(redisOptions)
	stream := "getName"
	group := "getName-group"
	ctx := context.Background()
	rqw, _ := NewBufferWrite(ctx, client, stream, group).(*BufferWrite)
	assert.Equal(t, stream, rqw.GetName())

}

func Test_GetLag(t *testing.T) {
	client := redisclient.NewRedisClient(redisOptions)
	stream := "getLag"
	group := "getLag-group"
	ctx := context.Background()
	rqw, _ := NewBufferWrite(ctx, client, stream, group).(*BufferWrite)
	assert.Equal(t, time.Duration(0), rqw.GetConsumerLag())
}

func Test_GetRefreshFullError(t *testing.T) {
	client := redisclient.NewRedisClient(redisOptions)
	group := "getRefreshFull-group"
	ctx := context.Background()
	rqw, _ := NewBufferWrite(ctx, client, "", group).(*BufferWrite)
	expected := uint32(3)
	assert.Equal(t, expected, rqw.GetRefreshFullError())
}

type myForwardRedisTest struct {
}

func (f myForwardRedisTest) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, nil
}

func (f myForwardRedisTest) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

// TestNewInterStepDataForwardRedis is used to read data from one step to another using redis as the Inter-Step Buffer
// For the purposes of testing we need to write some data to the from step
func TestNewInterStepDataForwardRedis(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	client := redisclient.NewRedisClient(redisOptions)
	fromGroup := "interstepdataforwardredis-from"
	toGroup := "interstepdataforwardredis-to"
	consumer := "interstepdataforwardredis-0"
	count := int64(20)
	fromStream := "fromStep"
	toStream := "toStep"

	// fromStep, we also have to create a fromStepWrite here to write data to the from stream
	fromStep, _ := NewBufferRead(ctx, client, fromStream, fromGroup, consumer, WithInfoRefreshInterval(2*time.Millisecond)).(*BufferRead)
	_ = client.CreateStreamGroup(ctx, fromStep.GetStreamName(), fromGroup, redisclient.ReadFromEarliest)
	fromStepWrite, _ := NewBufferWrite(ctx, client, fromStream, fromGroup, WithInfoRefreshInterval(2*time.Second), WithLagDuration(1*time.Millisecond)).(*BufferWrite)
	defer func() { _ = client.DeleteKeys(ctx, fromStepWrite.GetStreamName()) }()
	defer func() { _ = client.DeleteKeys(ctx, fromStep.GetStreamName()) }()
	defer func() { _ = client.DeleteStreamGroup(ctx, fromStep.GetStreamName(), fromGroup) }()

	// toStep, we also have to create a toStepRead here to read from the toStep
	to1Read, _ := NewBufferRead(ctx, client, toStream, toGroup, consumer).(*BufferRead)
	_ = client.CreateStreamGroup(ctx, to1Read.GetStreamName(), toGroup, redisclient.ReadFromEarliest)
	to1, _ := NewBufferWrite(ctx, client, toStream, toGroup, WithLagDuration(1*time.Millisecond), WithInfoRefreshInterval(2*time.Second), WithMaxLength(17)).(*BufferWrite)
	defer func() { _ = client.DeleteKeys(ctx, to1.GetStreamName()) }()
	defer func() { _ = client.DeleteKeys(ctx, to1.GetStreamName()) }()
	defer func() { _ = client.DeleteStreamGroup(ctx, to1.GetStreamName(), toGroup) }()

	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}

	writeMessages, internalKeys := buildTestWriteMessages(fromStepWrite, int64(20), testStartTime)

	defer func() { _ = client.DeleteKeys(ctx, internalKeys...) }()

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := forward.NewInterStepDataForward(vertex, fromStep, toSteps, myForwardRedisTest{}, myForwardRedisTest{}, fetchWatermark, publishWatermark)
	assert.NoError(t, err)
	assert.False(t, to1.IsFull())

	// forwardDataAndVerify is used to verify if data is fowarded from the from and received in the to buffer.
	forwardDataAndVerify(ctx, t, fromStepWrite, to1Read, to1, fromStep, f, writeMessages, count)

}

// TestReadTimeout tests that even though we have a blocking read, our Stop function exits cleanly
func TestReadTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	client := redisclient.NewRedisClient(redisOptions)
	fromGroup := "test-from"
	toGroup := "test-to"
	consumer := "test-0"
	fromStream := "from-st"
	toStream := "to-st"

	// fromStep, we also have to create a fromStepWrite here to write data to the from stream
	fromStep, _ := NewBufferRead(ctx, client, fromStream, fromGroup, consumer, WithInfoRefreshInterval(2*time.Millisecond)).(*BufferRead)
	_ = client.CreateStreamGroup(ctx, fromStep.GetStreamName(), fromGroup, redisclient.ReadFromEarliest)
	defer func() { _ = client.DeleteKeys(ctx, fromStep.GetStreamName()) }()
	defer func() { _ = client.DeleteStreamGroup(ctx, fromStep.GetStreamName(), fromGroup) }()

	to1, _ := NewBufferWrite(ctx, client, toStream, toGroup, WithLagDuration(1*time.Millisecond), WithInfoRefreshInterval(2*time.Second)).(*BufferWrite)
	_ = client.CreateStreamGroup(ctx, to1.GetStreamName(), toGroup, redisclient.ReadFromEarliest)

	defer func() { _ = client.DeleteKeys(ctx, to1.GetStreamName()) }()
	defer func() { _ = client.DeleteStreamGroup(ctx, to1.GetStreamName(), toGroup) }()

	toSteps := map[string]isb.BufferWriter{
		"to1": to1,
	}
	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := forward.NewInterStepDataForward(vertex, fromStep, toSteps, myForwardRedisTest{}, myForwardRedisTest{}, fetchWatermark, publishWatermark)
	assert.NoError(t, err)
	stopped := f.Start()
	// Call stop to end the test as we have a blocking read. The forwarder is up and running with no messages written
	f.Stop()

	<-stopped
}

// TestXTrimOnIsFull is used to verify if XTRIM is being called on isFull
func TestXTrimOnIsFull(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	client := redisclient.NewRedisClient(redisOptions)
	group := "trim-group"
	buffer := "trim"

	rqw, _ := NewBufferWrite(ctx, client, buffer, group, WithLagDuration(1*time.Millisecond), WithInfoRefreshInterval(2*time.Millisecond), WithMaxLength(10)).(*BufferWrite)
	err := client.CreateStreamGroup(ctx, rqw.GetStreamName(), group, redisclient.ReadFromEarliest)
	assert.NoError(t, err)

	defer func() { _ = client.DeleteStreamGroup(ctx, rqw.GetStreamName(), group) }()
	defer func() { _ = client.DeleteKeys(ctx, rqw.GetStreamName()) }()

	// Add some data
	startTime := time.Unix(1636470000, 0)
	messages := testutils.BuildTestWriteMessages(int64(10), startTime)
	// Add 10 messages
	for _, msg := range messages {
		err := client.Client.XAdd(ctx, &redis.XAddArgs{
			Stream: rqw.GetStreamName(),
			Values: []interface{}{msg.Header, msg.Body.Payload},
		}).Err()
		assert.NoError(t, err)
	}

	// Verify if buffer is full.
	for !rqw.IsFull() {
		select {
		case <-ctx.Done():
			t.Fatalf("not full, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	// Buffer is full at this point so write will fail with errors because of usage limit
	_, errs := rqw.Write(ctx, messages)
	for _, err := range errs {
		assert.Equal(t, err, isb.BufferWriteErr{Name: buffer, Full: true, Message: "Buffer full!"})
	}

	// Read all the messages.
	rqr, _ := NewBufferRead(ctx, client, buffer, group, "consumer").(*BufferRead)

	defer func() { _ = client.DeleteKeys(ctx, rqr.GetStreamName()) }()

	readMessages, err := rqr.Read(ctx, 10)
	// ACK all the messages
	var readOffsets = make([]string, len(readMessages))
	for idx, readMessage := range readMessages {
		readOffsets[idx] = readMessage.ReadOffset.String()
	}
	err = client.Client.XAck(redisclient.RedisContext, rqr.GetStreamName(), group, readOffsets...).Err()
	assert.NoError(t, err)

	// XTRIM should kick in and MINID is set to the last successfully processed message and should delete everything before that
	// so here we expect it to be at 1
	assert.True(t, testutils.ReadMessagesLen(ctx, redisOptions, rqw.GetStreamName(), 1))

}

// TestSetWriteInfo is used to test setWriteInfo
func TestSetWriteInfo(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	client := redisclient.NewRedisClient(redisOptions)
	group := "setWriteInfo-group"
	buffer := "setWriteInfo"

	rqw, _ := NewBufferWrite(ctx, client, buffer, group, WithLagDuration(1*time.Millisecond), WithInfoRefreshInterval(2*time.Millisecond), WithRefreshBufferWriteInfo(false)).(*BufferWrite)
	err := client.CreateStreamGroup(ctx, rqw.GetStreamName(), group, redisclient.ReadFromEarliest)
	assert.NoError(t, err)

	defer func() { _ = client.DeleteStreamGroup(ctx, rqw.GetStreamName(), group) }()
	defer func() { _ = client.DeleteKeys(ctx, rqw.GetStreamName()) }()

	// Add some data
	startTime := time.Unix(1636470000, 0)
	messages := testutils.BuildTestWriteMessages(int64(10), startTime)
	// Add 10 messages
	for _, msg := range messages {
		err := client.Client.XAdd(ctx, &redis.XAddArgs{
			Stream: rqw.GetStreamName(),
			Values: []interface{}{msg.Header, msg.Body.Payload},
		}).Err()
		assert.NoError(t, err)
	}

	// Read all the messages.
	rqr, _ := NewBufferRead(ctx, client, buffer, group, "consumer").(*BufferRead)

	defer func() { _ = client.DeleteKeys(ctx, rqr.GetStreamName()) }()

	readMessages, err := rqr.Read(ctx, 10)
	// ACK all the messages
	var readOffsets = make([]string, 1)

	// ACK 1 message
	readOffsets[0] = readMessages[0].ReadOffset.String()
	err = client.Client.XAck(redisclient.RedisContext, rqr.GetStreamName(), group, readOffsets...).Err()
	assert.NoError(t, err)

	rqw.setWriteInfo(ctx)

	assert.Equal(t, int64(9), rqw.GetPendingCount())
	assert.Equal(t, int64(10), rqw.GetBufferLength())
	assert.Equal(t, time.Duration(0), rqw.GetConsumerLag())
	assert.False(t, rqw.HasUnprocessedData())

}

// forwardDataAndVerify start the forwarder and verify the data.
func forwardDataAndVerify(ctx context.Context, t *testing.T, fromStepWrite *BufferWrite, to1Read *BufferRead, to1 *BufferWrite, fromStep *BufferRead, f *forward.InterStepDataForward, writeMessages []isb.Message, count int64) {
	stopped := f.Start()
	// write some data
	_, errs := fromStepWrite.Write(ctx, writeMessages[0:5])
	assert.Equal(t, make([]error, 5), errs)

	// validate the length of the toStep stream.
	assert.True(t, testutils.ReadMessagesLen(ctx, redisOptions, to1.GetStreamName(), 5))

	// write some more data: around 10 elements
	_, errs = fromStepWrite.Write(ctx, writeMessages[5:15])

	// read data back after asserting that the stream has received all the messages it was supposed to receive.
	// Here we added another 10 elements on top of the 5 so the total length is 15.
	assert.True(t, testutils.ReadMessagesLen(ctx, redisOptions, to1.GetStreamName(), 15))

	// write some data
	_, errs = fromStepWrite.Write(ctx, writeMessages[15:17])

	// give some time to assert, forwarding will take couple few cycles
	// and the best way is to wait till a buffer becomes full
	for !to1.IsFull() {
		select {
		case <-ctx.Done():
			t.Fatalf("not full, %s", ctx.Err())
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}
	assert.True(t, to1.IsFull())

	assert.True(t, testutils.ReadMessagesLen(ctx, redisOptions, to1.GetStreamName(), 17))

	readMessages, err := to1Read.Read(ctx, count)
	assert.NoError(t, err, "expected no error")

	// Read messages and validate the content
	assert.Len(t, readMessages, 17)
	assert.Equal(t, []interface{}{writeMessages[0].Header.PaneInfo, writeMessages[1].Header.PaneInfo, writeMessages[2].Header.PaneInfo, writeMessages[3].Header.PaneInfo, writeMessages[4].Header.PaneInfo}, []interface{}{readMessages[0].Header.PaneInfo, readMessages[1].Header.PaneInfo, readMessages[2].Header.PaneInfo, readMessages[3].Header.PaneInfo, readMessages[4].Header.PaneInfo})
	assert.Equal(t, []interface{}{writeMessages[0].Body.Payload, writeMessages[1].Body.Payload, writeMessages[2].Body.Payload, writeMessages[3].Body.Payload, writeMessages[4].Body.Payload}, []interface{}{readMessages[0].Body.Payload, readMessages[1].Body.Payload, readMessages[2].Body.Payload, readMessages[3].Body.Payload, readMessages[4].Body.Payload})

	_, _ = fromStepWrite.Write(ctx, writeMessages[18:20])

	f.Stop()

	<-stopped
}
