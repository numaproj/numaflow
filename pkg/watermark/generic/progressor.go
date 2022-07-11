package generic

import (
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// Progressor interface defines how the watermark can be progressed.
// FIXME: delete this
type Progressor interface {
	fetch.Fetcher
	publish.Publisher
}
