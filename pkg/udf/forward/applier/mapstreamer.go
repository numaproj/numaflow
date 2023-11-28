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

// MapStreamApplier applies the map stream udf on the read message and gives back response. Any UserError will be retried here, while
// InternalErr can be returned and could be retried by the callee.
type MapStreamApplier interface {
	ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error
}

// ApplyMapStreamFunc  utility function used to create a MapStreamApplier implementation
type ApplyMapStreamFunc func(context.Context, *isb.ReadMessage, chan<- isb.WriteMessage) error

func (f ApplyMapStreamFunc) ApplyMapStream(ctx context.Context, message *isb.ReadMessage, writeMessageCh chan<- isb.WriteMessage) error {
	return f(ctx, message, writeMessageCh)
}
