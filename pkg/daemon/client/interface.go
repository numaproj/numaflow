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

package client

import (
	"context"
	"io"

	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
)

type DaemonClient interface {
	io.Closer
	IsDrained(ctx context.Context, pipeline string) (bool, error)
	ListPipelineBuffers(ctx context.Context, pipeline string) ([]*daemon.BufferInfo, error)
	GetPipelineBuffer(ctx context.Context, pipeline, buffer string) (*daemon.BufferInfo, error)
	GetVertexMetrics(ctx context.Context, pipeline, vertex string) ([]*daemon.VertexMetrics, error)
	GetPipelineWatermarks(ctx context.Context, pipeline string) ([]*daemon.EdgeWatermark, error)
	GetPipelineStatus(ctx context.Context, pipeline string) (*daemon.PipelineStatus, error)
}
