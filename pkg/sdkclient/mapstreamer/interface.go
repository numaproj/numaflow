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

package mapstreamer

import (
	"context"

	v1 "github.com/numaproj/numaflow-go/pkg/apis/proto/mapstream/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client contains methods to call a gRPC client.
type Client interface {
	CloseConn(ctx context.Context) error
	IsReady(ctx context.Context, in *emptypb.Empty) (bool, error)
	MapStreamFn(ctx context.Context, request *v1.MapStreamRequest, responseCh chan<- *v1.MapStreamResponse) error
}
