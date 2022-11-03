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

package reducer

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/pbq/partition"
)

// Reducer applies the HTTPBasedUDF on the read message and gives back a new message. Any UserError will be retried here, while
// InternalErr can be returned and could be retried by the callee.
type Reducer interface {
	Reduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error)
}

// ReduceFunc utility function used to create a Reducer implementation
type ReduceFunc func(context.Context, *partition.ID, <-chan *isb.ReadMessage) ([]*isb.Message, error)

func (a ReduceFunc) Reduce(ctx context.Context, partitionID *partition.ID, messageStream <-chan *isb.ReadMessage) ([]*isb.Message, error) {
	return a(ctx, partitionID, messageStream)
}
