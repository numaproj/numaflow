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

package applier

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
)

// SourceTransformApplier applies the source transform on the read message and gives back a new message. Any UserError will be retried here, while
// InternalErr can be returned and could be retried by the callee.
type SourceTransformApplier interface {
	ApplyTransform(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error)
}

// ApplySourceTransformFunc is a function type that implements SourceTransformApplier interface.
type ApplySourceTransformFunc func(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error)

// ApplyTransform implements SourceTransformApplier interface.
func (f ApplySourceTransformFunc) ApplyTransform(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	return f(ctx, messages)
}
