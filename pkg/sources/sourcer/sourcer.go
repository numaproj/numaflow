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

package sourcer

import (
	"context"
	"io"

	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
)

type SourceReader interface {
	io.Closer
	// GetName returns the name of the source.
	GetName() string
	// Read reads a chunk of messages and returns at the first occurrence of an error. Error does not indicate that the
	// array of result is empty, the callee should process all the elements in the array even if the error is set. Read
	// will not mark the message in the buffer as "READ" if the read for that index is erring.
	// There is a chance that we have read the message and the container got forcefully terminated before processing. To provide
	// at-least-once semantics for reading, during the restart we will have to reprocess all unacknowledged messages.
	Read(context.Context, int64) ([]*isb.ReadMessage, error)
	// Ack acknowledges an array of offset.
	Ack(context.Context, []isb.Offset) []error
	// Partitions returns the partitions of the source. This is used by the forwarder to determine to which partition
	// idle watermarks should be published. Partition assignment to a pod is dynamic, so this method may return different
	// partitions at different times. (Example - Kafka, every time topic rebalancing happens, the partitions gets updated)
	Partitions() []int32
}

// Sourcer interface provides an isb.BufferReader abstraction over the underlying data source.
// This is intended to be consumed by a connector like isb.forward
type Sourcer interface {
	SourceReader
	forwarder.StarterStopper
	isb.LagReader
}
