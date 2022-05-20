package sources

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
)

// Sourcer interface provides a isb.BufferReader abstraction over the underlying data source.
// This is intended to be consumed by a connector like isb.forward
type Sourcer interface {
	isb.BufferReader
	forward.StarterStopper
}
