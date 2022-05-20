package sinks

import (
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
)

// Sinker interface defines what a Sink should implement.
type Sinker interface {
	isb.BufferWriter
	forward.StarterStopper
}
