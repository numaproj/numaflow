package store

import (
	"context"
)

// WatermarkStorer defines a pair of heartbeat KV store and offset timeline KV store
type WatermarkStorer interface {
	HeartbeatStore() WatermarkKVStorer
	OffsetTimelineStore() WatermarkKVStorer
}

// WatermarkKVStorer defines the storage for publishing the watermark.
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
	// KVPut indicates an element has been put/added into the KV store.
	KVPut KVWatchOp = iota
	// KVDelete represents a delete.
	KVDelete
	// KVPurge is when the kv bucket is purged.
	// This value is only for JetStream.
	KVPurge
)

func (kvOp KVWatchOp) String() string {
	switch kvOp {
	case KVPut:
		return "KVPut"
	case KVDelete:
		return "KVDelete"
	case KVPurge:
		return "KVPurge"
	default:
		return "UnknownOP"
	}
}

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
	// Watch starts the watermark kv watcher and returns a kv updates channel and a watcher stopped channel.
	Watch(context.Context) (<-chan WatermarkKVEntry, <-chan struct{})
	GetKVName() string
	Close()
}

// WatermarkStoreWatcher defines a pair of heartbeat KV watcher and offset timeline KV watcher
type WatermarkStoreWatcher interface {
	HeartbeatWatcher() WatermarkKVWatcher
	OffsetTimelineWatcher() WatermarkKVWatcher
}
