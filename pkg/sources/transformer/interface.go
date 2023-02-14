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

package transformer

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
)

type Transformer interface {
	// Transform applies source data transformations on a batch of isb.ReadMessage.
	// it's used by source vertex mainly to 1. assign to message a user defined event time, which is used for watermark propagation.
	// and 2. to update message key which enables early data filtering.
	// As a batch operation, Transform doesn't support returning partial data.
	// It either successfully transforms all input messages and returns full result, or return an empty result if there is any error.
	Transform(context.Context, []*isb.ReadMessage) []*isb.ReadMessage
}
