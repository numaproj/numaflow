package forward

import (
	"time"

	"go.uber.org/zap"
)

// options for forwarding the message
type options struct {
	// readBatchSize is the default batch size
	readBatchSize int64
	// udfConcurrency sets the concurrency for concurrent UDF processing
	udfConcurrency int
	// retryInterval is the time.Duration to sleep before retrying
	retryInterval time.Duration
	// logger is used to pass the logger variable
	logger *zap.SugaredLogger
}

type Option func(*options) error

// WithRetryInterval sets the retry interval
func WithRetryInterval(f time.Duration) Option {
	return func(o *options) error {
		o.retryInterval = time.Duration(f)
		return nil
	}
}

// WithReadBatchSize sets the read batch size
func WithReadBatchSize(f int64) Option {
	return func(o *options) error {
		o.readBatchSize = f
		return nil
	}
}

// WithUDFConcurrency ste concurrency for UDF processing
func WithUDFConcurrency(f int) Option {
	return func(o *options) error {
		o.udfConcurrency = f
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
