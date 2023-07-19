package jetstream

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
)

// KeyWatcher is a proxy struct to nats.KeyWatcher
// The existence of this proxy is to replace underlying nats.KeyWatcher
// with new one after reconnection.
type KeyWatcher struct {
	sync.RWMutex
	kvw nats.KeyWatcher
}

func (kw *KeyWatcher) Context() context.Context {
	kw.RLock()
	defer kw.RUnlock()
	return kw.kvw.Context()
}

func (kw *KeyWatcher) Updates() <-chan nats.KeyValueEntry {
	kw.RLock()
	defer kw.RUnlock()
	return kw.kvw.Updates()
}

func (kw *KeyWatcher) Stop() error {
	kw.RLock()
	defer kw.RUnlock()
	return kw.kvw.Stop()
}

func (kw *KeyWatcher) RefreshWatcher(watcher nats.KeyWatcher) {
	kw.Lock()
	defer kw.Unlock()
	kw.kvw = watcher
}
