package sourcetransformer

import (
	"context"
	"time"
)

// Datum contains methods to get the payload information.
type Datum interface {
	// Value returns the payload of the message.
	Value() []byte
	// EventTime returns the event time of the message.
	EventTime() time.Time
	// Watermark returns the watermark of the message.
	Watermark() time.Time
	// Headers returns the headers of the message.
	Headers() map[string]string
}

// SourceTransformer is the interface of SourceTransformer function implementation.
type SourceTransformer interface {
	// Transform is the function to transform each coming message.
	Transform(ctx context.Context, keys []string, datum Datum) Messages
}

// SourceTransformFunc is a utility type used to convert a function to a SourceTransformer.
type SourceTransformFunc func(ctx context.Context, keys []string, datum Datum) Messages

// Transform implements the function of source transformer function.
func (mf SourceTransformFunc) Transform(ctx context.Context, keys []string, datum Datum) Messages {
	return mf(ctx, keys, datum)
}
