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

// package redis

// import (
// 	"context"
// 	"testing"

// 	"github.com/redis/go-redis/v9"
// 	"github.com/stretchr/testify/assert"
// )

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

func TestNewRedisClient(t *testing.T) {
	t.SkipNow()
	ctx := context.TODO()
	redisOptions := &redis.UniversalOptions{
		Addrs: []string{":6379"},
	}
	client := NewRedisClient(redisOptions)
	var stream = "foo"
	var streamGroup = "foo-group"
	err := client.CreateStreamGroup(ctx, stream, streamGroup, ReadFromEarliest)
	assert.NoError(t, err)
	defer func() {
		err := client.DeleteStreamGroup(ctx, stream, streamGroup)
		assert.NoError(t, err)
	}()

	err = client.CreateStreamGroup(ctx, stream, streamGroup, ReadFromEarliest)
	assert.Error(t, err)
}

// Mock environment variables
func setEnv(key, value string) {
	os.Setenv(key, value)
}

func unsetEnv(key string) {
	os.Unsetenv(key)
}

func TestNewInClusterRedisClient(t *testing.T) {
	// Set environment variables
	setEnv(v1alpha1.EnvISBSvcRedisUser, "user")
	setEnv(v1alpha1.EnvISBSvcRedisPassword, "password")
	setEnv(v1alpha1.EnvISBSvcRedisURL, ":6379")
	setEnv(v1alpha1.EnvISBSvcRedisClusterMaxRedirects, "5")

	client := NewInClusterRedisClient()
	assert.NotNil(t, client)

	// Cleanup environment variables
	unsetEnv(v1alpha1.EnvISBSvcRedisUser)
	unsetEnv(v1alpha1.EnvISBSvcRedisPassword)
	unsetEnv(v1alpha1.EnvISBSvcRedisURL)
	unsetEnv(v1alpha1.EnvISBSvcRedisClusterMaxRedirects)
}

func TestErrorHelpers(t *testing.T) {
	errExist := fmt.Errorf("BUSYGROUP Consumer Group name already exists")
	assert.True(t, IsAlreadyExistError(errExist))

	errNotFound := fmt.Errorf("requires the key to exist")
	assert.True(t, NotFoundError(errNotFound))
}

func TestGetRedisStreamName(t *testing.T) {
	streamName := "test-stream"
	expected := "{test-stream}"
	got := GetRedisStreamName(streamName)
	assert.Equal(t, expected, got)
}

// Define a mock Redis client
type MockRedisClient struct {
	mock.Mock
	redis.UniversalClient
}

func (m *MockRedisClient) XGroupCreateMkStream(ctx context.Context, stream string, group string, start string) *redis.StatusCmd {
	args := m.Called(ctx, stream, group, start)
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetErr(args.Error(0))
	return cmd
}

func (m *MockRedisClient) XGroupDestroy(ctx context.Context, stream string, group string) *redis.IntCmd {
	args := m.Called(ctx, stream, group)
	cmd := redis.NewIntCmd(ctx)
	cmd.SetErr(args.Error(0))
	return cmd
}

func (m *MockRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	args := m.Called(ctx, keys)
	cmd := redis.NewIntCmd(ctx)
	cmd.SetErr(args.Error(0))
	return cmd
}

func (m *MockRedisClient) XInfoStream(ctx context.Context, stream string) *redis.XInfoStreamCmd {
	args := m.Called(ctx, stream)
	cmd := redis.NewXInfoStreamCmd(ctx, stream)
	if streamInfo, ok := args.Get(1).(*redis.XInfoStream); ok {
		cmd.SetVal(streamInfo)
	}
	cmd.SetErr(args.Error(0))
	return cmd
}

func (m *MockRedisClient) XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd {
	args := m.Called(ctx, stream)
	cmd := redis.NewXInfoGroupsCmd(ctx, stream)
	if groups, ok := args.Get(1).([]redis.XInfoGroup); ok {
		cmd.SetVal(groups)
	}
	cmd.SetErr(args.Error(0))
	return cmd
}

func (m *MockRedisClient) XPending(ctx context.Context, stream, group string) *redis.XPendingCmd {
	args := m.Called(ctx, stream, group)
	cmd := redis.NewXPendingCmd(ctx, stream, group)
	if pendingInfo, ok := args.Get(1).(*redis.XPending); ok {
		cmd.SetVal(pendingInfo)
	}
	cmd.SetErr(args.Error(0))
	return cmd
}

// Test suite for RedisClient
type RedisClientTestSuite struct {
	suite.Suite
	client *RedisClient
	mock   *MockRedisClient
}

func (suite *RedisClientTestSuite) SetupTest() {
	suite.mock = new(MockRedisClient)
	suite.client = &RedisClient{
		Client: suite.mock,
	}
}

func (suite *RedisClientTestSuite) TestCreateStreamGroup_Success() {
	suite.mock.On("XGroupCreateMkStream", mock.Anything, "mystream", "mygroup", "0").Return(nil)

	err := suite.client.CreateStreamGroup(context.Background(), "mystream", "mygroup", "0")

	suite.NoError(err)
	suite.mock.AssertCalled(suite.T(), "XGroupCreateMkStream", mock.Anything, "mystream", "mygroup", "0")
}

func (suite *RedisClientTestSuite) TestCreateStreamGroup_Error() {
	suite.mock.On("XGroupCreateMkStream", mock.Anything, "mystream", "mygroup", "0").Return(errors.New("error"))

	err := suite.client.CreateStreamGroup(context.Background(), "mystream", "mygroup", "0")

	suite.Error(err)
	suite.mock.AssertCalled(suite.T(), "XGroupCreateMkStream", mock.Anything, "mystream", "mygroup", "0")
}

func (suite *RedisClientTestSuite) TestDeleteStreamGroup_Success() {
	suite.mock.On("XGroupDestroy", mock.Anything, "mystream", "mygroup").Return(nil)

	err := suite.client.DeleteStreamGroup(context.Background(), "mystream", "mygroup")

	suite.NoError(err)
	suite.mock.AssertCalled(suite.T(), "XGroupDestroy", mock.Anything, "mystream", "mygroup")
}

func (suite *RedisClientTestSuite) TestDeleteStreamGroup_Error() {
	suite.mock.On("XGroupDestroy", mock.Anything, "mystream", "mygroup").Return(errors.New("error"))

	err := suite.client.DeleteStreamGroup(context.Background(), "mystream", "mygroup")

	suite.Error(err)
	suite.mock.AssertCalled(suite.T(), "XGroupDestroy", mock.Anything, "mystream", "mygroup")
}

func (suite *RedisClientTestSuite) TestDeleteKeys_Success() {
	suite.mock.On("Del", mock.Anything, []string{"key1", "key2"}).Return(nil)

	err := suite.client.DeleteKeys(context.Background(), "key1", "key2")

	suite.NoError(err)
	suite.mock.AssertCalled(suite.T(), "Del", mock.Anything, []string{"key1", "key2"})
}

func (suite *RedisClientTestSuite) TestDeleteKeys_Error() {
	suite.mock.On("Del", mock.Anything, []string{"key1", "key2"}).Return(errors.New("error"))

	err := suite.client.DeleteKeys(context.Background(), "key1", "key2")

	suite.Error(err)
	suite.mock.AssertCalled(suite.T(), "Del", mock.Anything, []string{"key1", "key2"})
}

func (suite *RedisClientTestSuite) TestStreamInfo_Success() {
	info := &redis.XInfoStream{
		Length: 5,
	}
	suite.mock.On("XInfoStream", mock.Anything, "mystream").Return(nil, info)

	result, err := suite.client.StreamInfo(context.Background(), "mystream")

	suite.NoError(err)
	suite.Equal(int64(5), result.Length)
	suite.mock.AssertCalled(suite.T(), "XInfoStream", mock.Anything, "mystream")
}

func (suite *RedisClientTestSuite) TestStreamInfo_Error() {
	suite.mock.On("XInfoStream", mock.Anything, "mystream").Return(errors.New("error"), nil)

	result, err := suite.client.StreamInfo(context.Background(), "mystream")

	suite.Error(err)
	suite.Nil(result)
	suite.mock.AssertCalled(suite.T(), "XInfoStream", mock.Anything, "mystream")
}

func (suite *RedisClientTestSuite) TestStreamGroupInfo_Success() {
	groupInfo := []redis.XInfoGroup{
		{Name: "group1", Consumers: 2},
	}
	suite.mock.On("XInfoGroups", mock.Anything, "mystream").Return(nil, groupInfo)

	result, err := suite.client.StreamGroupInfo(context.Background(), "mystream")

	suite.NoError(err)
	suite.Equal("group1", result[0].Name)
	suite.mock.AssertCalled(suite.T(), "XInfoGroups", mock.Anything, "mystream")
}

func (suite *RedisClientTestSuite) TestStreamGroupInfo_Error() {
	suite.mock.On("XInfoGroups", mock.Anything, "mystream").Return(errors.New("error"), nil)

	result, err := suite.client.StreamGroupInfo(context.Background(), "mystream")

	suite.Error(err)
	suite.Nil(result)
	suite.mock.AssertCalled(suite.T(), "XInfoGroups", mock.Anything, "mystream")
}

func (suite *RedisClientTestSuite) TestIsStreamExists_True() {
	suite.mock.On("XInfoStream", mock.Anything, "mystream").Return(nil, &redis.XInfoStream{})

	exists := suite.client.IsStreamExists(context.Background(), "mystream")

	suite.True(exists)
	suite.mock.AssertCalled(suite.T(), "XInfoStream", mock.Anything, "mystream")
}

func (suite *RedisClientTestSuite) TestIsStreamExists_False() {
	suite.mock.On("XInfoStream", mock.Anything, "mystream").Return(errors.New("error"), nil)

	exists := suite.client.IsStreamExists(context.Background(), "mystream")

	suite.False(exists)
	suite.mock.AssertCalled(suite.T(), "XInfoStream", mock.Anything, "mystream")
}

func (suite *RedisClientTestSuite) TestPendingMsgCount_Success() {
	pending := &redis.XPending{
		Count: 10,
	}
	suite.mock.On("XPending", mock.Anything, "mystream", "mygroup").Return(nil, pending)

	count, err := suite.client.PendingMsgCount(context.Background(), "mystream", "mygroup")

	suite.NoError(err)
	suite.Equal(int64(10), count)
	suite.mock.AssertCalled(suite.T(), "XPending", mock.Anything, "mystream", "mygroup")
}

func (suite *RedisClientTestSuite) TestPendingMsgCount_Error() {
	suite.mock.On("XPending", mock.Anything, "mystream", "mygroup").Return(errors.New("error"), nil)

	count, err := suite.client.PendingMsgCount(context.Background(), "mystream", "mygroup")

	suite.Error(err)
	suite.Equal(int64(0), count)
	suite.mock.AssertCalled(suite.T(), "XPending", mock.Anything, "mystream", "mygroup")
}

func (suite *RedisClientTestSuite) TestIsStreamGroupExists_True() {
	groupInfo := []redis.XInfoGroup{
		{Name: "mygroup"},
	}
	suite.mock.On("XInfoGroups", mock.Anything, "mystream").Return(nil, groupInfo)

	exists := suite.client.IsStreamGroupExists(context.Background(), "mystream", "mygroup")

	suite.True(exists)
	suite.mock.AssertCalled(suite.T(), "XInfoGroups", mock.Anything, "mystream")
}

func (suite *RedisClientTestSuite) TestIsStreamGroupExists_False_By_Error() {
	suite.mock.On("XInfoGroups", mock.Anything, "mystream").Return(errors.New("error"), nil)

	exists := suite.client.IsStreamGroupExists(context.Background(), "mystream", "mygroup")

	suite.False(exists)
	suite.mock.AssertCalled(suite.T(), "XInfoGroups", mock.Anything, "mystream")
}

func (suite *RedisClientTestSuite) TestIsStreamGroupExists_False_By_Empty() {
	groupInfo := []redis.XInfoGroup{}
	suite.mock.On("XInfoGroups", mock.Anything, "mystream").Return(nil, groupInfo)

	exists := suite.client.IsStreamGroupExists(context.Background(), "mystream", "mygroup")

	suite.False(exists)
	suite.mock.AssertCalled(suite.T(), "XInfoGroups", mock.Anything, "mystream")
}

// Run the test suite
func TestRedisClientTestSuite(t *testing.T) {
	suite.Run(t, new(RedisClientTestSuite))
}
