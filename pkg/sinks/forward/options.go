package forward

import (
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// options for forwarding the message
type options struct {
	// readBatchSize is the default batch size
	readBatchSize int64
	// sinkConcurrency sets the concurrency for concurrent sink processing
	sinkConcurrency int
	// retryInterval is the time.Duration to sleep before retrying
	retryInterval time.Duration
	// logger is used to pass the logger variable
	logger *zap.SugaredLogger
}

type Option func(*options) error

func DefaultOptions() *options {
	return &options{
		readBatchSize:   dfv1.DefaultReadBatchSize,
		sinkConcurrency: dfv1.DefaultReadBatchSize,
		retryInterval:   time.Millisecond,
		logger:          logging.NewLogger(),
	}
}

// WithReadBatchSize sets the read batch size
func WithReadBatchSize(f int64) Option {
	return func(o *options) error {
		o.readBatchSize = f
		return nil
	}
}

// WithSinkConcurrency sets concurrency for map UDF processing
func WithSinkConcurrency(f int) Option {
	return func(o *options) error {
		o.sinkConcurrency = f
		return nil
	}
}

// WithRetryInterval sets the retry interval
func WithRetryInterval(f time.Duration) Option {
	return func(o *options) error {
		o.retryInterval = time.Duration(f)
		return nil
	}
}

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *options) error {
		o.logger = l
		return nil
	}
}
