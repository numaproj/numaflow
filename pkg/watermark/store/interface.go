package store

import (
	"context"
)

// WMStorer is the watermark store implementation.
type WMStorer interface {
	WatermarkKVStorer
}

// WatermarkKVStorer is defines the storage for publishing the watermark.
type WatermarkKVStorer interface {
	// GetAllKeys the keys from KV store.
	GetAllKeys(context.Context) ([]string, error)
	// DeleteKey deletes the key from KV store.
	DeleteKey(context.Context, string) error
	// PutKV inserts a key-value pair into the KV store.
	PutKV(context.Context, string, []byte) error
	// GetValue gets the value of the given key.
	GetValue(context.Context, string) ([]byte, error)
	// GetStoreName returns the bucket name of the KV store.
	GetStoreName() string
	// Close closes the backend connection
	Close()
}

// KVWatchOp is the operation as detected by the KV watcher.
type KVWatchOp int64

const (
	// KVPut indicates an element has been put/added into the KV store
	KVPut KVWatchOp = iota
	// KVDelete represents a delete
	KVDelete
	KVPurge
)

// WatermarkKVEntry defines what can be read on the Watch stream.
type WatermarkKVEntry interface {
	// Key is the key that was retrieved.
	Key() string
	// Value is the retrieved value.
	Value() []byte
	// Operation returns `KVWatchOp`.
	Operation() KVWatchOp
}

// WatermarkKVWatcher watches the KV bucket for watermark progression.
type WatermarkKVWatcher interface {
	Watch(context.Context) <-chan WatermarkKVEntry
	GetKVName() string
	Stop(context.Context)
}
