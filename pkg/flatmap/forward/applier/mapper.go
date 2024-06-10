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

	flatmappb "github.com/numaproj/numaflow-go/pkg/apis/proto/flatmap/v1"

	"github.com/numaproj/numaflow/pkg/flatmap/types"
)

// FlatmapApplier applies the GRPCBasedMapUDF on the stream of read messages and gives back a new message.
type FlatmapApplier interface {
	// ApplyMap applies the Map UDF on a batch of N requests and streams the ResponseFlatmap response.
	// It doesn't wait for the response for all the requests, before starting to send the responses back.
	// It returns a channel on which any errors occurring during the processing can be propagated
	ApplyMap(ctx context.Context, messageStream []*types.RequestFlatmap, writeChan chan<- *flatmappb.MapResponse) (chan struct{}, <-chan error)
}

// ApplyFlatmapFunc utility function used to create a FlatmapApplier implementation
type ApplyFlatmapFunc func(ctx context.Context, messageStream []*types.RequestFlatmap, writeChan chan<- *flatmappb.MapResponse) (chan struct{}, <-chan error)

func (f ApplyFlatmapFunc) ApplyMap(ctx context.Context, messageStream []*types.RequestFlatmap, writeChan chan<- *flatmappb.MapResponse) (chan struct{}, <-chan error) {
	return f(ctx, messageStream, writeChan)
}
