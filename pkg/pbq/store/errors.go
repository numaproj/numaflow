package store

import "errors"

var WriteStoreFullError error = errors.New("error while writing to store, store is full")
var WriteStoreClosedError error = errors.New("error while writing to store, store is closed")
var ReadStoreEmptyError error = errors.New("error while reading from store, store is empty")
