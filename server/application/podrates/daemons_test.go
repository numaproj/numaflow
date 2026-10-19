/*
Copyright 2026 The Numaproj Authors.

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

package podrates

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtxclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
)

func TestConcurrentCacheMissCreatesOneClient(t *testing.T) {
	originalMono := newMonoVertexDaemonClient
	originalPipeline := newPipelineDaemonClient
	t.Cleanup(func() {
		newMonoVertexDaemonClient = originalMono
		newPipelineDaemonClient = originalPipeline
	})

	var monoCreated atomic.Int32
	var pipelineCreated atomic.Int32
	newMonoVertexDaemonClient = func(string, string) (mvtxclient.MonoVertexDaemonClient, error) {
		monoCreated.Add(1)
		time.Sleep(30 * time.Millisecond)
		return &stubMonoClient{}, nil
	}
	newPipelineDaemonClient = func(string, string) (daemonclient.DaemonClient, error) {
		pipelineCreated.Add(1)
		time.Sleep(30 * time.Millisecond)
		return &stubPipelineClient{}, nil
	}

	clients, err := NewDaemonClients("grpc")
	if err != nil {
		t.Fatalf("NewDaemonClients: %v", err)
	}

	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers * 2)
	monoResults := make([]mvtxclient.MonoVertexDaemonClient, callers)
	pipelineResults := make([]daemonclient.DaemonClient, callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			client, err := clients.MonoVertexDaemon("ns", "orders")
			if err != nil {
				t.Errorf("MonoVertexDaemon: %v", err)
				return
			}
			monoResults[i] = client
		}(i)
		go func(i int) {
			defer wg.Done()
			client, err := clients.PipelineDaemon("ns", "orders")
			if err != nil {
				t.Errorf("PipelineDaemon: %v", err)
				return
			}
			pipelineResults[i] = client
		}(i)
	}
	wg.Wait()

	if got := monoCreated.Load(); got != 1 {
		t.Fatalf("MonoVertex dials = %d, want 1", got)
	}
	if got := pipelineCreated.Load(); got != 1 {
		t.Fatalf("pipeline dials = %d, want 1", got)
	}
	for i := 1; i < callers; i++ {
		if monoResults[i] != monoResults[0] {
			t.Fatalf("MonoVertex callers received different clients")
		}
		if pipelineResults[i] != pipelineResults[0] {
			t.Fatalf("pipeline callers received different clients")
		}
	}

	if _, err := clients.MonoVertexDaemon("ns", "orders"); err != nil {
		t.Fatalf("cached MonoVertexDaemon: %v", err)
	}
	if _, err := clients.PipelineDaemon("ns", "orders"); err != nil {
		t.Fatalf("cached PipelineDaemon: %v", err)
	}
	if monoCreated.Load() != 1 || pipelineCreated.Load() != 1 {
		t.Fatal("a cache hit dialed again")
	}
}

type stubMonoClient struct{}

func (s *stubMonoClient) Close() error { return nil }
func (s *stubMonoClient) GetMonoVertexMetrics(context.Context) (*mvtxdaemon.MonoVertexMetrics, error) {
	return nil, nil
}
func (s *stubMonoClient) GetMonoVertexPodReadRates(context.Context) ([]*mvtxdaemon.PodReadRate, error) {
	return nil, nil
}
func (s *stubMonoClient) GetMonoVertexStatus(context.Context) (*mvtxdaemon.MonoVertexStatus, error) {
	return nil, nil
}
func (s *stubMonoClient) GetMonoVertexErrors(context.Context, string) ([]*mvtxdaemon.ReplicaErrors, error) {
	return nil, nil
}

type stubPipelineClient struct{}

func (s *stubPipelineClient) Close() error { return nil }
func (s *stubPipelineClient) IsDrained(context.Context, string) (bool, error) {
	return false, nil
}
func (s *stubPipelineClient) ListPipelineBuffers(context.Context, string) ([]*daemon.BufferInfo, error) {
	return nil, nil
}
func (s *stubPipelineClient) GetPipelineBuffer(context.Context, string, string) (*daemon.BufferInfo, error) {
	return nil, nil
}
func (s *stubPipelineClient) GetVertexMetrics(context.Context, string, string) ([]*daemon.VertexMetrics, error) {
	return nil, nil
}
func (s *stubPipelineClient) GetVertexPodReadRates(context.Context, string, string) ([]*daemon.PodReadRate, error) {
	return nil, nil
}
func (s *stubPipelineClient) GetPipelineWatermarks(context.Context, string) ([]*daemon.EdgeWatermark, error) {
	return nil, nil
}
func (s *stubPipelineClient) GetPipelineStatus(context.Context, string) (*daemon.PipelineStatus, error) {
	return nil, nil
}
func (s *stubPipelineClient) GetVertexErrors(context.Context, string, string) ([]*daemon.ReplicaErrors, error) {
	return nil, nil
}
