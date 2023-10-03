/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pbq

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
)

// ReadWriteCloser is an unified interface to PBQ read and write interfaces. Close is only for Writer.
type ReadWriteCloser interface {
	Reader
	WriteCloser
}

// Reader provides methods to read from PBQ.
type Reader interface {
	// ReadCh exposes channel to read from PBQ
	ReadCh() <-chan *isb.ReadMessage
	// GC does garbage collection, it deletes all the persisted data from the store
	GC() error
}

// WriteCloser provides methods to write data to the PQB and close the PBQ.
// No data can be written to PBQ after cob.
type WriteCloser interface {
	// Write writes message to PBQ
	Write(ctx context.Context, msg *isb.ReadMessage) error
	// CloseOfBook (cob) closes PBQ, no writes will be accepted after cob
	CloseOfBook()
	// Close to handle context close on writer
	// Any pending data can be flushed to the persistent store at this point.
	Close() error
}
