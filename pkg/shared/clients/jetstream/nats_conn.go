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

package jetstream

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// NatsConn is a wrapper of nats.Conn, which implements our own magic for auto reconnection.
type NatsConn struct {
	Conn *nats.Conn

	// pingConext is a dedicated JetStreamContext used to check if connection is OK.
	pingContext nats.JetStreamContext
	// contextMap stores all the JetStreamContext created through this connection.
	contextMap map[int64]*JetStreamContext
	// jsOptMap stores all the options used for JetStreamContext creation.
	jsOptMap map[int64][]nats.JSOpt
	lock     *sync.RWMutex
}

// NewNatsConn returns a NatsConn instance
func NewNatsConn(conn *nats.Conn) *NatsConn {
	return &NatsConn{
		Conn:       conn,
		contextMap: make(map[int64]*JetStreamContext),
		jsOptMap:   make(map[int64][]nats.JSOpt),
		lock:       new(sync.RWMutex),
	}
}

// Close function closes the underlying Nats connection.
func (nc *NatsConn) Close() {
	if !nc.Conn.IsClosed() {
		nc.Conn.Close()
	}
}

// JetStream function invokes same function of underlying Nats connection for returning,
// meanwhile store the JetStreamContext for restoration after reconnection.
func (nc *NatsConn) JetStream(opts ...nats.JSOpt) (*JetStreamContext, error) {
	js, err := nc.Conn.JetStream(opts...)
	if err != nil {
		return nil, err
	}
	jsc := &JetStreamContext{
		js: js,
	}
	nc.lock.Lock()
	_key := time.Now().UnixNano()
	nc.contextMap[_key] = jsc
	nc.jsOptMap[_key] = opts
	nc.lock.Unlock()
	return jsc, nil
}

// IsClosed is a simple proxy invocation.
func (nc *NatsConn) IsClosed() bool {
	return nc.Conn.IsClosed()
}

// IsConnected function implements the magic to check if the connection is OK.
// It utilize the dedicated JetStreamContext to call AccountInfo() function,
// and check if it works for determination. To reduce occasionality, it checks
// 3 times if there's a failure.
func (nc *NatsConn) IsConnected() bool {
	if nc.Conn == nil || nc.Conn.IsClosed() || !nc.Conn.IsConnected() { // This is not good enough, sometimes IsConnected() can not detect dropped connection
		return false
	}
	if nc.pingContext == nil {
		nc.pingContext, _ = nc.Conn.JetStream()
	}
	i := 0
	retryCount := 3
	failed := true
retry:
	for i < retryCount && failed {
		i++
		failed = false
		if _, err := nc.pingContext.AccountInfo(); err != nil {
			failed = true
			fmt.Printf("Error to ping Nats JetStream server: %v\n", err)
			time.Sleep(500 * time.Millisecond)
			goto retry
		}
	}
	return !failed
}

// reloadContexts is a function to recreate JetStreamContext after reconnection.
func (nc *NatsConn) reloadContexts() {
	if nc.Conn == nil || nc.Conn.IsClosed() || !nc.Conn.IsConnected() {
		return
	}
	nc.pingContext, _ = nc.Conn.JetStream()
	for k, v := range nc.contextMap {
		opts := nc.jsOptMap[k]
		js, _ := nc.Conn.JetStream(opts...)
		v.js = js
	}
}
