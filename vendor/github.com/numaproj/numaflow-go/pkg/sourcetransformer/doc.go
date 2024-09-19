// Package sourcetransformer implements the server code for Source Transformer in golang.
//
// Example Transform (extracting event time from the datum payload)
// Transform includes both Map and EventTime assignment functionalities.
// Although the input datum already contains EventTime and Watermark, it's up to the Transform implementor to
// decide on whether to use them for generating new EventTime.
// Transform can be used only at source vertex by source data transformer.

// Examples: https://github.com/numaproj/numaflow-go/tree/main/pkg/sourcetransformer/examples/

package sourcetransformer
