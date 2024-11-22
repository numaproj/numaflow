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

package mapper

import (
	"context"

	mappb "github.com/numaproj/numaflow-go/pkg/apis/proto/map/v1"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Client contains methods to call a gRPC client.
type Client interface {
	CloseConn() error
	IsReady(ctx context.Context, in *emptypb.Empty) (bool, error)
	MapFn(ctx context.Context, mapRequest []*mappb.MapRequest) ([]*mappb.MapResponse, error)
	MapStreamFn(ctx context.Context, request *mappb.MapRequest, responseCh chan<- *mappb.MapResponse) error
}
