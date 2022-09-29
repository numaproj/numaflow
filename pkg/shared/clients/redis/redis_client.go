package redis

import (
	"context"
	"os"
	"strings"

	"github.com/go-redis/redis/v8"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

const ReadFromEarliest = "0-0"

// RedisContext is used to pass the context specifically for REDIS operations.
// A cancelled context during SIGTERM or Ctrl-C that is propagated down will throw a context cancelled error because redis uses context to obtain connection from the connection pool.
// All redis operations will use the below no-op context.Background() to try to process in-flight messages that we have received prior to the cancellation of the context.
var RedisContext = context.Background()

// RedisClient datatype to hold redis client attributes.
type RedisClient struct {
	Client redis.UniversalClient
}

// NewRedisClient returns a new Redis Client.
func NewRedisClient(options *redis.UniversalOptions) *RedisClient {
	client := new(RedisClient)
	client.Client = redis.NewUniversalClient(options)
	return client
}

// NewInClusterRedisClient returns a new Redis Client, it assumes it's in a vertex pod,
// where those required environment variables are available.
func NewInClusterRedisClient() *RedisClient {
	opts := &redis.UniversalOptions{
		Username:   os.Getenv(v1alpha1.EnvISBSvcRedisUser),
		Password:   os.Getenv(v1alpha1.EnvISBSvcRedisPassword),
		MasterName: os.Getenv(v1alpha1.EnvISBSvcSentinelMaster),
	}
	if opts.MasterName != "" {
		urls := os.Getenv(v1alpha1.EnvISBSvcRedisSentinelURL)
		if urls != "" {
			opts.Addrs = strings.Split(urls, ",")
		}
		opts.SentinelPassword = os.Getenv(v1alpha1.EnvISBSvcRedisSentinelPassword)
	} else {
		urls := os.Getenv(v1alpha1.EnvISBSvcRedisURL)
		if urls != "" {
			opts.Addrs = strings.Split(urls, ",")
		}
	}
	return NewRedisClient(opts)
}

// CreateStreamGroup creates a redis stream group and creates an empty stream if it does not exist.
func (cl *RedisClient) CreateStreamGroup(ctx context.Context, stream string, group string, start string) error {
	return cl.Client.XGroupCreateMkStream(ctx, stream, group, start).Err()
}

// DeleteStreamGroup deletes the redis stream group.
func (cl *RedisClient) DeleteStreamGroup(ctx context.Context, stream string, group string) error {
	return cl.Client.XGroupDestroy(ctx, stream, group).Err()
}

// DeleteKeys deletes a redis keys
func (cl *RedisClient) DeleteKeys(ctx context.Context, keys ...string) error {
	return cl.Client.Del(ctx, keys...).Err()
}

// StreamInfo returns redis stream info
func (cl *RedisClient) StreamInfo(ctx context.Context, streamKey string) (*redis.XInfoStream, error) {
	return cl.Client.XInfoStream(ctx, streamKey).Result()
}

// StreamGroupInfo returns redis stream group info
func (cl *RedisClient) StreamGroupInfo(ctx context.Context, streamKey string) ([]redis.XInfoGroup, error) {
	return cl.Client.XInfoGroups(ctx, streamKey).Result()
}

// IsStreamExists check the redis keys exists
func (cl *RedisClient) IsStreamExists(ctx context.Context, streamKey string) bool {
	_, err := cl.StreamInfo(ctx, streamKey)
	return err == nil
}

// PendingMsgCount check the redis keys exists
func (cl *RedisClient) PendingMsgCount(ctx context.Context, streamKey, consumerGroup string) (int64, error) {
	cmd := cl.Client.XPending(ctx, streamKey, consumerGroup)
	pending, err := cmd.Result()
	if err != nil {
		return 0, err
	}
	return pending.Count, nil
}

// IsStreamGroupExists check the stream group exists
func (cl *RedisClient) IsStreamGroupExists(ctx context.Context, streamKey string, groupName string) bool {
	result, err := cl.StreamGroupInfo(ctx, streamKey)
	if err != nil {
		return false
	}
	if len(result) == 0 {
		return false
	}
	for _, groupInfo := range result {
		if groupInfo.Name == groupName {
			return true
		}
	}
	return false
}

func IsAlreadyExistError(err error) bool {
	return strings.Contains(err.Error(), "BUSYGROUP")
}

func NotFoundError(err error) bool {
	return strings.Contains(err.Error(), "requires the key to exist")
}
