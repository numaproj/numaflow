package fetch

import (
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
)

// _bucketWatchRetryCount is max number of retry to make sure we can connect to a bucket to watch.
const _bucketWatchRetryCount = 5

// _delayInSecBetweenBucketWatchRetry in seconds between bucket watch retry
const _delayInSecBetweenBucketWatchRetry = 1

// RetryUntilSuccessfulWatcherCreation creates a watcher and will wait till it is created if infiniteLoop is set to true.
// TODO: use `wait.ExponentialBackoffWithContext`
func RetryUntilSuccessfulWatcherCreation(js *jsclient.JetStreamContext, bucketName string, infiniteLoop bool, log *zap.SugaredLogger) nats.KeyWatcher {
	for i := 0; i < _bucketWatchRetryCount || infiniteLoop; i++ {
		bucket, err := js.KeyValue(bucketName)
		if err != nil {
			log.Errorw("failed to get the bucket by bucket name", zap.String("bucket", bucketName), zap.Error(err))
			time.Sleep(_delayInSecBetweenBucketWatchRetry * time.Second)
			continue
		}
		watcher, err := bucket.WatchAll()
		if err != nil {
			log.Errorw("failed to create the watch all watcher for bucket name", zap.String("bucket", bucketName), zap.Error(err))
			time.Sleep(_delayInSecBetweenBucketWatchRetry * time.Second)
			continue
		}
		log.Infow("watcher created for bucket", zap.String("bucket", bucketName))
		return watcher
	}
	return nil
}
