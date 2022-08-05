package jetstream

import (
	"time"
)

// jsClientOptions is a struct of the options for JetStream client.
type jsClientOptions struct {
	reconnect               bool
	connectionCheckInterval time.Duration
	reconnectHandler        func(*NatsConn)
	disconnectHandler       func(*NatsConn, error)
}

// defaultJetStreamClientOptions returns a default instance of jsClientOptions.
func defaultJetStreamClientOptions() *jsClientOptions {
	return &jsClientOptions{
		reconnect:               true,
		connectionCheckInterval: 6 * time.Second,
	}
}

type JetStreamClientOption func(*jsClientOptions)

// NoReconnect is an Option to set no auto reconnect.
func NoReconnect() JetStreamClientOption {
	return func(opts *jsClientOptions) {
		opts.reconnect = false
	}
}

// ConnectionCheckInterval is an Option to set connection check interval.
func ConnectionCheckInterval(d time.Duration) JetStreamClientOption {
	return func(opts *jsClientOptions) {
		opts.connectionCheckInterval = d
	}
}

// ReconnectHandler is an Option to set reconnect handler.
func ReconnectHandler(f func(*NatsConn)) JetStreamClientOption {
	return func(opts *jsClientOptions) {
		opts.reconnectHandler = f
	}
}

// DisconnectErrHandler is an option to set disconnect handler.
func DisconnectErrHandler(f func(*NatsConn, error)) JetStreamClientOption {
	return func(opts *jsClientOptions) {
		opts.disconnectHandler = f
	}
}
