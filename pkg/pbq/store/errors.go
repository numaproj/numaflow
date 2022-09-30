package store

import "errors"

var WriteStoreFullErr error = errors.New("error writing, store is full")
var WriteStoreClosedErr error = errors.New("error writing, store is closed")
var ReadStoreEmptyErr error = errors.New("error reading, store is empty")
