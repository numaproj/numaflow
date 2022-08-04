package jetstream

import (
	"time"
)

// Options for JetStream client
type jsClientOptions struct {
	reconnect               bool
	connectionCheckInterval time.Duration
	reconnectHandler        func(*NatsConn)
	disconnectHandler       func(*NatsConn, error)
}

func defaultJetStreamClientOptions() *jsClientOptions {
	return &jsClientOptions{
		reconnect:               false,
		connectionCheckInterval: 5 * time.Second,
	}
}

type JetStreamClientOption func(*jsClientOptions)

// Set auto reconnect
func AutoReconnect() JetStreamClientOption {
	return func(opts *jsClientOptions) {
		opts.reconnect = true
	}
}

// Set connection check interval
func ConnectionCheckInterval(d time.Duration) JetStreamClientOption {
	return func(opts *jsClientOptions) {
		opts.connectionCheckInterval = d
	}
}

// Set reconnect handler
func ReconnectHandler(f func(*NatsConn)) JetStreamClientOption {
	return func(opts *jsClientOptions) {
		opts.reconnectHandler = f
	}
}

// Set disconnect handler
func DisconnectErrHandler(f func(*NatsConn, error)) JetStreamClientOption {
	return func(opts *jsClientOptions) {
		opts.disconnectHandler = f
	}
}
