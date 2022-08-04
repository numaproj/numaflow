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
		reconnect:               true,
		connectionCheckInterval: 6 * time.Second,
	}
}

type JetStreamClientOption func(*jsClientOptions)

// Set no auto reconnect
func NoReconnect() JetStreamClientOption {
	return func(opts *jsClientOptions) {
		opts.reconnect = false
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
