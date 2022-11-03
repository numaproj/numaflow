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
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/generic"

	"github.com/go-redis/redis/v8"
	"github.com/montanaflynn/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/isb/testutils"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
)

var (
	redisOptions = &redis.UniversalOptions{
		//Addrs: []string{":7000", ":7001", ":7002", ":7003", ":7004", ":7005"}
		Addrs: []string{":6379"},
	}

	testStartTime = time.Unix(1636470000, 0).UTC()
)

func TestRedisQRead_Read(t *testing.T) {
	ctx := context.Background()
	client := redisclient.NewRedisClient(redisOptions)
	stream := "somestream"
	group := "testgroup1"
	consumer := "con-0"

	count := int64(10)
	rqr, _ := NewBufferRead(ctx, client, stream, group, consumer).(*BufferRead)
	err := client.CreateStreamGroup(ctx, rqr.GetStreamName(), group, redisclient.ReadFromEarliest)
	assert.NoError(t, err)

	defer func() { _ = client.DeleteStreamGroup(ctx, rqr.GetStreamName(), group) }()
	defer func() { _ = client.DeleteKeys(ctx, rqr.GetStreamName()) }()

	// Add some data
	startTime := time.Unix(1636470000, 0)
	messages := testutils.BuildTestWriteMessages(count, startTime)
	for _, msg := range messages {
		err := client.Client.XAdd(ctx, &redis.XAddArgs{
			Stream: rqr.GetStreamName(),
			Values: []interface{}{msg.Header, msg.Body.Payload},
		}).Err()
		assert.NoError(t, err)
	}

	readMessages, err := rqr.Read(ctx, count)
	assert.NoErrorf(t, err, "rqr.Read failed, %s", err)
	assert.Len(t, readMessages, int(count))
}

func TestRedisCheckBacklog(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := redisclient.NewRedisClient(redisOptions)
	stream := "readbacklog"
	group := "readbacklog-group"
	consumer := "readbacklog-consumer"

	count := int64(10)
	rqr, _ := NewBufferRead(ctx, client, stream, group, consumer).(*BufferRead)
	err := client.CreateStreamGroup(ctx, rqr.GetStreamName(), group, redisclient.ReadFromEarliest)
	assert.NoError(t, err)

	defer func() { _ = client.DeleteStreamGroup(ctx, rqr.GetStreamName(), group) }()
	defer func() { _ = client.DeleteKeys(ctx, rqr.GetStreamName()) }()

	// Add some data
	startTime := time.Unix(1636470000, 0)
	messages := testutils.BuildTestWriteMessages(count, startTime)
	for _, msg := range messages {
		err := client.Client.XAdd(ctx, &redis.XAddArgs{
			Stream: rqr.GetStreamName(),
			Values: []interface{}{msg.Header, msg.Body.Payload},
		}).Err()
		assert.NoError(t, err)
	}

	readMessages, err := rqr.Read(ctx, count)
	// check if backlog is set to false
	assert.False(t, rqr.checkBackLog)
	assert.NoErrorf(t, err, "rqr.Read failed, %s", err)
	assert.Len(t, readMessages, int(count))

	rqr.options.checkBackLog = true

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	rqw, _ := NewBufferWrite(ctx, client, "toStream", "toGroup", WithInfoRefreshInterval(2*time.Millisecond), WithLagDuration(time.Minute)).(*BufferWrite)
	err = client.CreateStreamGroup(ctx, rqw.GetStreamName(), "toGroup", redisclient.ReadFromEarliest)
	assert.NoError(t, err)

	defer func() { _ = client.DeleteStreamGroup(ctx, rqw.GetStreamName(), "toGroup") }()
	defer func() { _ = client.DeleteKeys(ctx, rqw.GetStreamName()) }()
	toSteps := map[string]isb.BufferWriter{
		"to1": rqw,
	}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	f, err := forward.NewInterStepDataForward(vertex, rqr, toSteps, forwardReadWritePerformance{}, forwardReadWritePerformance{}, fetchWatermark, publishWatermark, forward.WithReadBatchSize(10))

	stopped := f.Start()
	// validate the length of the toStep stream.
	assert.True(t, testutils.ReadMessagesLen(ctx, redisOptions, rqw.GetStreamName(), count))

	f.Stop()

	<-stopped

}

// Reads the data written to Redis Streams.
type ReadTestSuite struct {
	suite.Suite
	ctx     context.Context
	rclient *redisclient.RedisClient
	rqr     *BufferRead
	rqw     *BufferWrite
	count   int64
}

func (suite *ReadTestSuite) SetupSuite() {
	client := redisclient.NewRedisClient(redisOptions)
	ctx := context.Background()
	stream := "testsuitestream"
	group := "testsuitegroup1"
	consumer := "testsuite-0"
	count := int64(10)
	rqw, _ := NewBufferWrite(ctx, client, stream, group).(*BufferWrite)
	rqr, _ := NewBufferRead(ctx, client, stream, group, consumer).(*BufferRead)

	suite.ctx = ctx
	suite.rclient = client
	suite.rqw = rqw
	suite.rqr = rqr
	suite.count = count
}

func (suite *ReadTestSuite) TearDownSuite() {
}

// run before each test
func (suite *ReadTestSuite) SetupTest() {
	err := suite.rclient.CreateStreamGroup(suite.ctx, suite.rqr.Stream, suite.rqr.Group, redisclient.ReadFromEarliest)
	// better fail early
	_ = suite.NoError(err) || suite.Failf("CreateStreamGroup failed", "%s", err)
}

// run after before each test
func (suite *ReadTestSuite) TearDownTest() {
	err := suite.rclient.DeleteStreamGroup(suite.ctx, suite.rqr.Stream, suite.rqr.Group)
	suite.NoError(err, "DeleteStreamGroup failed")
	err = suite.rclient.DeleteKeys(suite.ctx, suite.rqr.Stream)
	suite.NoError(err, "DeleteKeys failed")
}

// TestRead first writes and then reads back
func (suite *ReadTestSuite) TestRead() {
	count := int64(10)
	writeMessages, internalKeys := buildTestWriteMessages(suite.rqw, count, testStartTime)
	defer func() { _ = suite.rclient.DeleteKeys(suite.ctx, internalKeys...) }()

	_, errs := suite.rqw.Write(suite.ctx, writeMessages)
	suite.Equal(make([]error, len(writeMessages)), errs, "Write failed")

	readMessages, err := suite.rqr.Read(suite.ctx, suite.count)
	suite.NoErrorf(err, "rqr.Read failed, %s", err)
	suite.Len(readMessages, int(count))

	messagesInsideReadMessages := make([]isb.Message, len(writeMessages))
	offsetsInsideReadMessages := make([]isb.Offset, len(writeMessages))
	for idx, readMessage := range readMessages {
		messagesInsideReadMessages[idx] = readMessage.Message
		offsetsInsideReadMessages[idx] = readMessage.ReadOffset
	}
	suite.Equal(writeMessages, messagesInsideReadMessages, "writeMessages and readMessages should be same")

	// XPending == Read message length as we have not ack'ed yet
	res, err := suite.rclient.Client.XPending(suite.ctx, suite.rqr.Stream, suite.rqr.Group).Result()
	suite.NoError(err)
	suite.Equal(count, res.Count)

	errs = suite.rqr.Ack(suite.ctx, offsetsInsideReadMessages)
	for _, e := range errs {
		suite.NoError(e, "Ack should succeed")
	}

	res, err = suite.rclient.Client.XPending(suite.ctx, suite.rqr.Stream, suite.rqr.Group).Result()
	suite.NoError(err)
	suite.Equal(int64(0), res.Count)
}

// TestReadWithDuplicateWrite first writes twice where second is duplicate and then reads back
func (suite *ReadTestSuite) TestReadWithDuplicateWrite() {
	count := int64(10)
	writeMessages, internalKeys := buildTestWriteMessages(suite.rqw, count, testStartTime)
	defer func() { _ = suite.rclient.DeleteKeys(suite.ctx, internalKeys...) }()

	// first time write
	_, errs := suite.rqw.Write(suite.ctx, writeMessages)
	suite.Equal(make([]error, len(writeMessages)), errs, "Write failed")
	// duplicate write
	_, errs = suite.rqw.Write(suite.ctx, writeMessages)
	suite.Equal(make([]error, len(writeMessages)), errs, "Write failed")

	readMessages, err := suite.rqr.Read(suite.ctx, suite.count)
	suite.NoErrorf(err, "rqr.Read failed, %s", err)
	suite.Len(readMessages, int(count))

	messagesInsideReadMessages := make([]isb.Message, len(writeMessages))
	offsetsInsideReadMessages := make([]isb.Offset, len(writeMessages))
	for idx, readMessage := range readMessages {
		messagesInsideReadMessages[idx] = readMessage.Message
		offsetsInsideReadMessages[idx] = readMessage.ReadOffset
	}
	suite.Equal(writeMessages, messagesInsideReadMessages, "writeMessages and readMessages should be same")

	// XPending == Read message length as we have not ack'ed yet
	res, err := suite.rclient.Client.XPending(suite.ctx, suite.rqr.Stream, suite.rqr.Group).Result()
	suite.NoError(err)
	suite.Equal(count, res.Count)

	errs = suite.rqr.Ack(suite.ctx, offsetsInsideReadMessages)
	for _, e := range errs {
		suite.NoError(e, "Ack should succeed")
	}

	res, err = suite.rclient.Client.XPending(suite.ctx, suite.rqr.Stream, suite.rqr.Group).Result()
	suite.NoError(err)
	suite.Equal(int64(0), res.Count)
}

func (suite *ReadTestSuite) Test_GetRefreshEmptyError() {
	expected := uint32(1)
	assert.Equal(suite.T(), expected, suite.rqr.GetRefreshEmptyError())
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestReadTestSuite(t *testing.T) {
	suite.Run(t, new(ReadTestSuite))
}

// ReadWritePerformance is to do end to end performance testing using isb.forward to move messages.
type ReadWritePerformance struct {
	suite.Suite
	ctx            context.Context
	rclient        *redisclient.RedisClient
	rqr            *BufferRead
	rqw            *BufferWrite
	isdf           *forward.InterStepDataForward
	count          int64
	withPipelining bool
	cancel         context.CancelFunc
}

type forwardReadWritePerformance struct {
}

func (f forwardReadWritePerformance) WhereTo(_ string) ([]string, error) {
	return []string{"to1"}, nil
}

func (f forwardReadWritePerformance) Apply(ctx context.Context, message *isb.ReadMessage) ([]*isb.Message, error) {
	return testutils.CopyUDFTestApply(ctx, message)
}

func (suite *ReadWritePerformance) SetupSuite() {
	client := redisclient.NewRedisClient(redisOptions)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	fromStream := "ReadWritePerformance-from"
	toStream := "ReadWritePerformance-to"
	fromGroup := "ReadWritePerformance-group-from"
	toGroup := "ReadWritePerformance-group-to"
	consumer := "ReadWritePerformance-con-0"
	count := int64(10000)
	rqw, _ := NewBufferWrite(ctx, client, toStream, toGroup, WithInfoRefreshInterval(2*time.Millisecond), WithLagDuration(time.Minute), WithMaxLength(20000)).(*BufferWrite)
	rqr, _ := NewBufferRead(ctx, client, fromStream, fromGroup, consumer).(*BufferRead)

	toSteps := map[string]isb.BufferWriter{
		"to1": rqw,
	}

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}

	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	isdf, _ := forward.NewInterStepDataForward(vertex, rqr, toSteps, forwardReadWritePerformance{}, forwardReadWritePerformance{}, fetchWatermark, publishWatermark)

	suite.ctx = ctx
	suite.rclient = client
	suite.rqw = rqw
	suite.rqr = rqr
	suite.isdf = isdf
	suite.count = count
	suite.cancel = cancel
}

func (suite *ReadWritePerformance) TearDownSuite() {
	suite.cancel()
}

// run before each test
func (suite *ReadWritePerformance) SetupTest() {
	err := suite.rclient.CreateStreamGroup(suite.ctx, suite.rqr.GetStreamName(), suite.rqr.GetGroupName(), redisclient.ReadFromEarliest)
	// better fail early
	_ = suite.NoError(err) || suite.Failf("CreateStreamGroup failed", "%s", err)
	err = suite.rclient.CreateStreamGroup(suite.ctx, suite.rqw.GetStreamName(), suite.rqw.GetGroupName(), redisclient.ReadFromEarliest)
	_ = suite.NoError(err) || suite.Failf("CreateStreamGroup failed", "%s", err)
}

// run after before each test
func (suite *ReadWritePerformance) TearDownTest() {
	err := suite.rclient.DeleteStreamGroup(suite.ctx, suite.rqr.GetStreamName(), suite.rqr.GetGroupName())
	suite.NoError(err, "DeleteStreamGroup failed")
	err = suite.rclient.DeleteStreamGroup(suite.ctx, suite.rqw.GetStreamName(), suite.rqw.GetGroupName())
	suite.NoError(err, "DeleteStreamGroup failed")
	err = suite.rclient.DeleteKeys(suite.ctx, suite.rqr.GetStreamName())
	suite.NoError(err, "DeleteKeys failed")
	err = suite.rclient.DeleteKeys(suite.ctx, suite.rqw.GetStreamName())
	suite.NoError(err, "DeleteKeys failed")
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestReadWritePerformanceSuite(t *testing.T) {
	suite.Run(t, new(ReadWritePerformance))
}

// TestReadWriteLatency is used to look at the latency during forward.
func (suite *ReadWritePerformance) TestReadWriteLatency() {
	_ = NewBufferRead(suite.ctx, suite.rclient, "ReadWritePerformance-to", "ReadWritePerformance-group-to", "consumer-0")
	suite.False(suite.rqw.IsFull())
	var writeMessages = make([]isb.Message, 0, suite.count)

	writeMessages = append(writeMessages, testutils.BuildTestWriteMessages(suite.count, testStartTime)...)

	stopped := suite.isdf.Start()

	writeTestMessages(suite.ctx, suite.rclient, writeMessages, suite.rqr.GetStreamName())

	// wait till all the messages are read by the to step
	suite.True(testutils.ReadMessagesLen(suite.ctx, redisOptions, suite.rqw.GetStreamName(), suite.count))

	suite.isdf.Stop()

	// XRANGE on the tostep.
	resultsTo := suite.rclient.Client.XRange(suite.ctx, suite.rqw.GetStreamName(), "-", "+")

	xMessages, err := resultsTo.Result()
	if err != nil {
		suite.Error(err)
	}

	latency := generateLatencySlice(xMessages, suite)
	fmt.Println(getPercentiles(suite.T(), latency))

	<-stopped
}

// TestReadWriteLatencyPipelining is performs wthe latency test during a forward.
func (suite *ReadWritePerformance) TestReadWriteLatencyPipelining() {
	suite.rqw, _ = NewBufferWrite(suite.ctx, suite.rclient, "ReadWritePerformance-to", "ReadWritePerformance-group-to", WithInfoRefreshInterval(2*time.Second), WithLagDuration(time.Minute), WithoutPipelining(), WithMaxLength(20000)).(*BufferWrite)
	_ = NewBufferRead(suite.ctx, suite.rclient, "ReadWritePerformance-to", "ReadWritePerformance-group-to", "consumer-0")

	vertex := &dfv1.Vertex{Spec: dfv1.VertexSpec{
		PipelineName: "testPipeline",
		AbstractVertex: dfv1.AbstractVertex{
			Name: "testVertex",
		},
	}}
	toSteps := map[string]isb.BufferWriter{
		"to1": suite.rqw,
	}
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferMap(toSteps)
	suite.isdf, _ = forward.NewInterStepDataForward(vertex, suite.rqr, toSteps, forwardReadWritePerformance{}, forwardReadWritePerformance{}, fetchWatermark, publishWatermark)

	suite.False(suite.rqw.IsFull())
	var writeMessages = make([]isb.Message, 0, suite.count)

	writeMessages = append(writeMessages, testutils.BuildTestWriteMessages(suite.count, testStartTime)...)

	stopped := suite.isdf.Start()

	writeTestMessages(suite.ctx, suite.rclient, writeMessages, suite.rqr.GetStreamName())

	// wait till all the messages are read by the to step
	suite.True(testutils.ReadMessagesLen(suite.ctx, redisOptions, suite.rqw.GetStreamName(), suite.count))

	suite.isdf.Stop()

	// XRANGE on the tostep.
	resultsTo := suite.rclient.Client.XRange(suite.ctx, suite.rqw.GetStreamName(), "-", "+")

	xMessages, err := resultsTo.Result()
	if err != nil {
		suite.Error(err)
	}

	latency := generateLatencySlice(xMessages, suite)
	fmt.Println(getPercentiles(suite.T(), latency))

	<-stopped
}

func getPercentiles(t *testing.T, latency []float64) string {
	min, err := stats.Min(latency)
	assert.NoError(t, err)
	p50, err := stats.Percentile(latency, 50)
	assert.NoError(t, err)
	p75, err := stats.Percentile(latency, 70)
	assert.NoError(t, err)
	p90, err := stats.Percentile(latency, 90)
	assert.NoError(t, err)
	p95, err := stats.Percentile(latency, 95)
	assert.NoError(t, err)
	p99, err := stats.Percentile(latency, 99)
	assert.NoError(t, err)
	max, err := stats.Max(latency)
	assert.NoError(t, err)

	return fmt.Sprintf("min=%f p50=%f p75=%f p90=%f p95=%f p99=%f max=%f", min, p50, p75, p90, p95, p99, max)
}

// writeTestMessages is used to add some dummy messages using XADD

func writeTestMessages(ctx context.Context, client *redisclient.RedisClient, messages []isb.Message, streamName string) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, message := range messages {
			client.Client.XAdd(ctx, &redis.XAddArgs{
				Stream: streamName,
				Values: []interface{}{message.Header, message.Body.Payload},
			})
		}
	}()
}

// generateLatencySlice is used to generate latency slice.
func generateLatencySlice(xMessages []redis.XMessage, suite *ReadWritePerformance) []float64 {
	latency := make([]float64, suite.count)
	for idx, xMessage := range xMessages {
		m := isb.Header{}
		for k := range xMessage.Values {
			err := json.Unmarshal([]byte(k), &m)
			suite.NoError(err)
		}
		id, err := splitId(xMessage.ID)
		offset, err := splitId(m.ID)
		suite.NoError(err)

		// We store a difference of the id and the offset in the to stream.
		//This gives us a difference between the time it was  read that is stored in ID of the Header and the time it was written as stored in the ID.
		latency[idx] = float64(id - offset)
	}

	return latency

}
