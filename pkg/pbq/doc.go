// Package pbq implements a persistent buffer queue datastructure that provides queue capabilities with persistence.

// A persistent buffer queue (pbq) is expected to be used in a stream processing pipeline to temporarily
// store the data while waiting for watermark progression. It only provides facilities to write to and
// read the data from a durable storage. A pbq is agnostic of keyed vs non-keyed streams.
// A pbq instance has a one-to-one mapping with a partition, which is a {key, window} tuple.

package pbq
