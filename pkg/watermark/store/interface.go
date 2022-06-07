package store

import "context"

// WMStorer is the watermark store implementation.
type WMStorer interface {
	PublisherKVStorer
}

// PublisherKVStorer is defines the storage for publishing the watermark.
type PublisherKVStorer interface {
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
}
