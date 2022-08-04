package jetstream

import (
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// NatsConn is a wrapper of nats.Conn, where we
type NatsConn struct {
	Conn *nats.Conn

	pingContext nats.JetStreamContext
	contextMap  map[int64]nats.JetStreamContext
	lock        *sync.RWMutex
}

func NewNatsConn(conn *nats.Conn) *NatsConn {
	return &NatsConn{
		Conn:       conn,
		contextMap: make(map[int64]nats.JetStreamContext),
		lock:       new(sync.RWMutex),
	}
}

func (nc *NatsConn) Close() {
	nc.Conn.Close()
}

func (nc *NatsConn) JetStream(opts ...nats.JSOpt) (nats.JetStreamContext, error) {
	js, err := nc.Conn.JetStream(opts...)
	if err != nil {
		return nil, err
	}
	nc.lock.Lock()
	nc.contextMap[time.Now().UnixMicro()] = js
	nc.lock.Unlock()
	return js, nil
}

func (nc *NatsConn) IsClosed() bool {
	return nc.Conn.IsClosed()
}

func (nc *NatsConn) IsConnected() bool {
	if nc.Conn == nil || nc.Conn.IsClosed() || !nc.Conn.IsConnected() {
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

func (nc *NatsConn) reloadContexts() {
	if nc.Conn == nil || nc.Conn.IsClosed() || !nc.Conn.IsConnected() {
		return
	}
	nc.pingContext, _ = nc.Conn.JetStream()
	for k := range nc.contextMap {
		js, _ := nc.Conn.JetStream()
		nc.contextMap[k] = js
	}
}
