package jetstream

import (
	"context"

	"github.com/nats-io/nats.go"
)

// KeyWatcher is a proxy struct to nats.KeyWatcher
// The existence of this proxy is to replace underlying nats.KeyWatcher
// with new one after reconnection.
type KeyWatcher struct {
	kvw nats.KeyWatcher
}

func (kw *KeyWatcher) Context() context.Context {
	return kw.kvw.Context()
}

func (kw *KeyWatcher) Updates() <-chan nats.KeyValueEntry {
	return kw.kvw.Updates()
}

func (kw *KeyWatcher) Stop() error {
	return kw.kvw.Stop()
}
