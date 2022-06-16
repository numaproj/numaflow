package jetstream

// TimestampedSequence is a helper struct to wrap a sequence ID and timestamp pair
type timestampedSequence struct {
	seq int64
	// timestamp in seconds
	timestamp int64
}
