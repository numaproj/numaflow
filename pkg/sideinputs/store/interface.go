package store

import (
	"context"
)

// SideInputKVEntry defines what can be read on the Watch stream.
type SideInputKVEntry interface {
	// Key is the key that was retrieved.
	Key() string
	// Value is the retrieved value.
	Value() []byte
}

// SideInputWatcher watches the KV bucket for Side Input data updates.
type SideInputWatcher interface {
	// Watch starts the watermark kv watcher and returns a kv updates channel and a watcher stopped channel.
	Watch(context.Context) (<-chan SideInputKVEntry, <-chan struct{})
	GetKVName() string
	Close()
}
