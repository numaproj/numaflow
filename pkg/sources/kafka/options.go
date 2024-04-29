package kafka

import (
	"time"

	"go.uber.org/zap"
)

type Option func(*kafkaSource) error

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *kafkaSource) error {
		o.logger = l
		return nil
	}
}

// WithBufferSize is used to return size of message channel information
func WithBufferSize(s int) Option {
	return func(o *kafkaSource) error {
		o.handlerBuffer = s
		return nil
	}
}

// WithReadTimeOut is used to set the read timeout for the from buffer
func WithReadTimeOut(t time.Duration) Option {
	return func(o *kafkaSource) error {
		o.readTimeout = t
		return nil
	}
}

// WithGroupName is used to set the group name
func WithGroupName(gn string) Option {
	return func(o *kafkaSource) error {
		o.groupName = gn
		return nil
	}
}
