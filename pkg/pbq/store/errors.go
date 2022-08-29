package store

import "errors"

var WriteStoreFullErr error = errors.New("error while writing to store, store is full")
var WriteStoreClosedErr error = errors.New("error while writing to store, store is closed")
var ReadStoreEmptyErr error = errors.New("error while reading from store, store is empty")
