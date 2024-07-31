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

// BatchMapApplier applies the batch map UDF on the read messages and gives back the responses for the whole
// batch
type BatchMapApplier interface {
	ApplyBatchMap(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error)
}

// BatchMapFunc utility function used to create a BatchMapApplier implementation
type BatchMapFunc func(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error)

func (f BatchMapFunc) ApplyBatchMap(ctx context.Context, messages []*isb.ReadMessage) ([]isb.ReadWriteMessagePair, error) {
	return f(ctx, messages)
}
