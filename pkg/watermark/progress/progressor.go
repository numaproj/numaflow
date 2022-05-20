package progress

import (
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

// Progressor interface defines how the watermark can be progressed.
type Progressor interface {
	fetch.Fetcher
	publish.Publisher
}
