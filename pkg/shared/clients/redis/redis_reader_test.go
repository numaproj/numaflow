package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"

	"github.com/numaproj/numaflow/pkg/isb"
)

// Mocking the dependencies

func setup() (*RedisStreamsRead, *redis.Client, *observer.ObservedLogs) {
	core, obs := observer.New(zap.DebugLevel)
	logger := zap.New(core).Sugar()

	mockClient := redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	r := &RedisStreamsRead{
		Name:         "testName",
		Stream:       "testStream",
		Group:        "testGroup",
		Consumer:     "testConsumer",
		PartitionIdx: 1,
		RedisClient: &RedisClient{
			Client: mockClient,
		},
		Options: Options{
			ReadTimeOut:  5 * time.Second,
			CheckBackLog: false,
		},
		Log: logger,
		Metrics: Metrics{
			ReadErrorsInc: func() {},
			ReadsAdd:      func(int) {},
			AcksAdd:       func(int) {},
			AckErrorsAdd:  func(int) {},
		},
		XStreamToMessages: func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
			return messages, nil
		},
	}

	return r, mockClient, obs
}

func Test_GetName(t *testing.T) {
	reader, _, _ := setup()
	assert.Equal(t, "testName", reader.GetName())
}

func Test_GetPartitionIdx(t *testing.T) {
	reader, _, _ := setup()
	assert.Equal(t, int32(1), reader.GetPartitionIdx())
}
func Test_GetStreamName(t *testing.T) {
	reader, _, _ := setup()
	assert.Equal(t, "testStream", reader.GetStreamName())
}
func Test_GetGroupName(t *testing.T) {
	reader, _, _ := setup()
	assert.Equal(t, "testGroup", reader.GetGroupName())
}

func TestRedisStreamsRead_processReadError(t *testing.T) {
	t.Run("context canceled error", func(t *testing.T) {
		loggerCore, _ := observer.New(zap.DebugLevel)
		logger := zap.New(loggerCore).Sugar()

		reader := &RedisStreamsRead{
			Options: Options{CheckBackLog: true},
			Log:     logger,
			XStreamToMessages: func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
				return messages, nil
			},
		}

		// Create a context canceled error
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		msgs, err := reader.processReadError(nil, nil, ctx.Err())
		assert.NoError(t, err)
		assert.Empty(t, msgs)
	})

	t.Run("redis nil error", func(t *testing.T) {
		loggerCore, _ := observer.New(zap.DebugLevel)
		logger := zap.New(loggerCore).Sugar()

		reader := &RedisStreamsRead{
			Options: Options{CheckBackLog: true},
			Log:     logger,
			XStreamToMessages: func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
				return messages, nil
			},
		}

		msgs, err := reader.processReadError(nil, nil, redis.Nil)
		assert.NoError(t, err)
		assert.Empty(t, msgs)
	})

	t.Run("generic error with metrics increment", func(t *testing.T) {
		loggerCore, _ := observer.New(zap.DebugLevel)
		logger := zap.New(loggerCore).Sugar()

		readErrorsIncCalled := false
		reader := &RedisStreamsRead{
			Options: Options{CheckBackLog: true},
			Log:     logger,
			Metrics: Metrics{
				ReadErrorsInc: func() {
					readErrorsIncCalled = true
				},
			},
			XStreamToMessages: func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
				return messages, errors.New("conversion error")
			},
		}

		msgs, err := reader.processReadError(nil, nil, errors.New("some error"))
		assert.Error(t, err)
		assert.Equal(t, "XReadGroup failed, some error", err.Error())
		assert.Empty(t, msgs)
		assert.True(t, readErrorsIncCalled)
	})

	t.Run("generic error without metrics increment", func(t *testing.T) {
		loggerCore, _ := observer.New(zap.DebugLevel)
		logger := zap.New(loggerCore).Sugar()

		reader := &RedisStreamsRead{
			Options: Options{CheckBackLog: true},
			Log:     logger,
			XStreamToMessages: func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
				return messages, errors.New("conversion error")
			},
		}

		msgs, err := reader.processReadError(nil, nil, errors.New("some error"))
		assert.Error(t, err)
		assert.Equal(t, "XReadGroup failed, some error", err.Error())
		assert.Empty(t, msgs)
	})
}

func TestRedisStreamsRead_NoAck(t *testing.T) {
	reader, _, _ := setup()

	offsets := []isb.Offset{isb.NewSimpleStringPartitionOffset("0-0", 0)}
	reader.NoAck(context.Background(), offsets)
	// NoAck does nothing, so just ensure no panic
}

// Mock implementation for isb.Offset
type SimpleOffset struct {
	offset       string
	partitionIdx int32
}

func (o SimpleOffset) String() string {
	return o.offset
}

func (o SimpleOffset) Sequence() (int64, error) {
	return 0, nil // Not implemented for our mock
}

func (o SimpleOffset) AckIt() error {
	return nil // Mock implementation
}

func (o SimpleOffset) NoAck() error {
	return nil // Mock implementation
}

func (o SimpleOffset) PartitionIdx() int32 {
	return o.partitionIdx
}

func (m *MockRedisClient) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	args := m.Called(ctx, a)
	cmd := redis.NewXStreamSliceCmd(ctx)
	cmd.SetVal(args.Get(0).([]redis.XStream))
	cmd.SetErr(args.Error(1))
	return cmd
}

func (m *MockRedisClient) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	args := m.Called(ctx, stream, group, ids)
	cmd := redis.NewIntCmd(ctx)
	cmd.SetErr(args.Error(0))
	return cmd
}

// Define custom metrics increments and add functions
type MockMetrics struct {
	mock.Mock
}

func (m *MockMetrics) ReadErrorsInc() {
	m.Called()
}

func (m *MockMetrics) ReadsAdd(count int) {
	m.Called(count)
}

func (m *MockMetrics) AcksAdd(count int) {
	m.Called(count)
}

func (m *MockMetrics) AckErrorsAdd(count int) {
	m.Called(count)
}

// Test suite for RedisStreamsRead
type RedisStreamsReadTestSuite struct {
	suite.Suite
	client  *RedisStreamsRead
	mock    *MockRedisClient
	metrics *MockMetrics
}

func (suite *RedisStreamsReadTestSuite) SetupTest() {
	suite.mock = new(MockRedisClient)
	suite.metrics = new(MockMetrics)
	logger := zaptest.NewLogger(suite.T())
	suite.client = &RedisStreamsRead{
		Name:         "test",
		Stream:       "mystream",
		Group:        "mygroup",
		Consumer:     "consumer1",
		PartitionIdx: 0,
		RedisClient: &RedisClient{
			Client: suite.mock,
		},
		Options: Options{
			CheckBackLog: true,
			ReadTimeOut:  10 * time.Second,
		},
		Log: logger.Sugar(),
		Metrics: Metrics{
			ReadErrorsInc: suite.metrics.ReadErrorsInc,
			ReadsAdd:      suite.metrics.ReadsAdd,
			AcksAdd:       suite.metrics.AcksAdd,
			AckErrorsAdd:  suite.metrics.AckErrorsAdd,
		},
		XStreamToMessages: func(xstreams []redis.XStream, messages []*isb.ReadMessage, labels map[string]string) ([]*isb.ReadMessage, error) {
			return messages, nil
		},
	}
}

func (suite *RedisStreamsReadTestSuite) TestRead_Success_Backlog() {
	xstreams := []redis.XStream{
		{Messages: []redis.XMessage{
			{ID: "1", Values: map[string]interface{}{"data": "value1"}},
		}},
	}
	suite.mock.On("XReadGroup", mock.Anything, mock.Anything).Return(xstreams, nil)
	suite.metrics.On("ReadsAdd", 1).Return()

	messages, err := suite.client.Read(context.Background(), 1)

	suite.NoError(err)
	suite.Equal(0, len(messages))
	suite.metrics.AssertCalled(suite.T(), "ReadsAdd", 1)
}

func (suite *RedisStreamsReadTestSuite) TestAck_Success() {
	offsets := []isb.Offset{SimpleOffset{offset: "1"}}
	dedupOffsets := []string{"1"}
	suite.mock.On("XAck", mock.Anything, "mystream", "mygroup", dedupOffsets).Return(nil)
	suite.metrics.On("AcksAdd", 1).Return()

	errs := suite.client.Ack(context.Background(), offsets)

	for _, err := range errs {
		suite.NoError(err)
	}
	suite.metrics.AssertCalled(suite.T(), "AcksAdd", 1)
}

func (suite *RedisStreamsReadTestSuite) TestAck_Error() {
	offsets := []isb.Offset{SimpleOffset{offset: "1"}}
	dedupOffsets := []string{"1"}
	suite.mock.On("XAck", mock.Anything, "mystream", "mygroup", dedupOffsets).Return(errors.New("ack error"))
	suite.metrics.On("AckErrorsAdd", 1).Return()

	errs := suite.client.Ack(context.Background(), offsets)

	for _, err := range errs {
		suite.Error(err)
		suite.Equal("ack error", err.Error())
	}
	suite.metrics.AssertCalled(suite.T(), "AckErrorsAdd", 1)
}

func (suite *RedisStreamsReadTestSuite) TestPending_Error() {
	suite.mock.On("XInfoGroups", mock.Anything, "mystream").Return(nil, errors.New("info error"))

	lag, err := suite.client.Pending(context.Background())

	suite.Error(err)
	suite.Equal(isb.PendingNotAvailable, lag)
}

// Run the test suite
func TestRedisStreamsReadTestSuite(t *testing.T) {
	suite.Run(t, new(RedisStreamsReadTestSuite))
}
