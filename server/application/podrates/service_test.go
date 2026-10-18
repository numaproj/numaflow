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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
	fakeclientset "github.com/numaproj/numaflow/pkg/client/clientset/versioned/fake"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtxclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
)

func TestGetMonoVertexPodReadRates(t *testing.T) {
	observed := time.Date(2026, time.October, 16, 12, 0, 0, 0, time.UTC)
	daemons := &fakeDaemonClients{mono: []*mvtxdaemon.PodReadRate{{
		Hostname:  "orders-ingest-mv-0",
		Replica:   0,
		ReadRates: map[string]*wrapperspb.DoubleValue{"5m": wrapperspb.Double(1.5)},
	}}}
	service := testService(t, daemons, &dfv1.MonoVertex{ObjectMeta: metav1.ObjectMeta{Name: "orders-ingest", Namespace: "team-a"}})
	service.now = func() time.Time { return observed }

	rates, err := service.GetMonoVertexPodReadRates(context.Background(), "team-a", "orders-ingest")
	require.NoError(t, err)
	assert.Equal(t, observed, rates.ObservedAt)
	require.Len(t, rates.Pods, 1)
	assert.Equal(t, "orders-ingest-mv-0", rates.Pods[0].Hostname)
	assert.Equal(t, 0, rates.Pods[0].Replica)
	require.NotNil(t, rates.Pods[0].Rate5m)
	assert.Equal(t, 1.5, *rates.Pods[0].Rate5m)
	assert.Nil(t, rates.Pods[0].Rate15m)
}

func TestGetMonoVertexPodReadRatesNotFound(t *testing.T) {
	service := testService(t, &fakeDaemonClients{}, &dfv1.MonoVertex{ObjectMeta: metav1.ObjectMeta{Name: "orders-ingest", Namespace: "team-a"}})
	_, err := service.GetMonoVertexPodReadRates(context.Background(), "team-a", "missing")
	assert.True(t, apierrors.IsNotFound(err))
}

func TestGetPipelineVertexPodReadRates(t *testing.T) {
	daemons := &fakeDaemonClients{pipeline: []*daemon.PodReadRate{{
		Hostname:  "orders-map-1",
		Replica:   1,
		ReadRates: map[string]*wrapperspb.DoubleValue{"15m": wrapperspb.Double(0)},
	}}}
	service := testService(t, daemons, &dfv1.Vertex{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-map", Namespace: "team-a"},
		Spec:       dfv1.VertexSpec{AbstractVertex: dfv1.AbstractVertex{Name: "map"}, PipelineName: "orders"},
	})

	rates, err := service.GetPipelineVertexPodReadRates(context.Background(), "team-a", "orders", "map")
	require.NoError(t, err)
	require.Len(t, rates.Pods, 1)
	assert.Equal(t, 1, rates.Pods[0].Replica)
	require.NotNil(t, rates.Pods[0].Rate15m)
	assert.Equal(t, 0.0, *rates.Pods[0].Rate15m)
	assert.Nil(t, rates.Pods[0].Rate5m)
}

func TestGetPipelineVertexPodReadRatesRejectsAmbiguousName(t *testing.T) {
	service := testService(t, &fakeDaemonClients{}, &dfv1.Vertex{
		ObjectMeta: metav1.ObjectMeta{Name: "orders-map", Namespace: "team-a"},
		Spec:       dfv1.VertexSpec{AbstractVertex: dfv1.AbstractVertex{Name: "other"}, PipelineName: "orders"},
	})
	_, err := service.GetPipelineVertexPodReadRates(context.Background(), "team-a", "orders", "map")
	assert.True(t, apierrors.IsNotFound(err))
}

func TestDaemonFailureIsUnavailable(t *testing.T) {
	daemons := &fakeDaemonClients{err: errors.New("connection refused")}
	service := testService(t, daemons, &dfv1.MonoVertex{ObjectMeta: metav1.ObjectMeta{Name: "orders-ingest", Namespace: "team-a"}})
	_, err := service.GetMonoVertexPodReadRates(context.Background(), "team-a", "orders-ingest")
	assert.ErrorIs(t, err, ErrDaemonUnavailable)
	assert.False(t, apierrors.IsNotFound(err))
}

func TestDaemonClientsReuseConnection(t *testing.T) {
	clients, err := NewDaemonClients("grpc")
	require.NoError(t, err)
	first, err := clients.MonoVertexDaemon("team-a", "orders-ingest")
	require.NoError(t, err)
	second, err := clients.MonoVertexDaemon("team-a", "orders-ingest")
	require.NoError(t, err)
	assert.Same(t, first, second)

	pipeline, err := clients.PipelineDaemon("team-a", "orders")
	require.NoError(t, err)
	again, err := clients.PipelineDaemon("team-a", "orders")
	require.NoError(t, err)
	assert.Same(t, pipeline, again)
}

func testService(t *testing.T, daemons DaemonClients, objects ...runtime.Object) *Service {
	t.Helper()
	clientset := fakeclientset.NewSimpleClientset()
	clientset.PrependReactor("get", "vertices", func(action k8stesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(k8stesting.GetAction)
		if !ok {
			return false, nil, nil
		}
		for _, object := range objects {
			vertex, ok := object.(*dfv1.Vertex)
			if ok && action.GetNamespace() == vertex.Namespace && getAction.GetName() == vertex.Name {
				return true, vertex.DeepCopy(), nil
			}
		}
		return false, nil, nil
	})
	clientset.PrependReactor("get", "monovertices", func(action k8stesting.Action) (bool, runtime.Object, error) {
		getAction, ok := action.(k8stesting.GetAction)
		if !ok {
			return false, nil, nil
		}
		for _, object := range objects {
			monoVertex, ok := object.(*dfv1.MonoVertex)
			if ok && action.GetNamespace() == monoVertex.Namespace && getAction.GetName() == monoVertex.Name {
				return true, monoVertex.DeepCopy(), nil
			}
		}
		return false, nil, nil
	})
	service, err := NewService(clientset.NumaflowV1alpha1(), daemons)
	require.NoError(t, err)
	return service
}

type fakeDaemonClients struct {
	mono     []*mvtxdaemon.PodReadRate
	pipeline []*daemon.PodReadRate
	err      error
}

func (f *fakeDaemonClients) MonoVertexDaemon(string, string) (mvtxclient.MonoVertexDaemonClient, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &fakeMonoClient{pods: f.mono}, nil
}

func (f *fakeDaemonClients) PipelineDaemon(string, string) (daemonclient.DaemonClient, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &fakePipelineClient{pods: f.pipeline}, nil
}

type fakeMonoClient struct {
	pods []*mvtxdaemon.PodReadRate
}

func (f *fakeMonoClient) Close() error { return nil }
func (f *fakeMonoClient) GetMonoVertexMetrics(context.Context) (*mvtxdaemon.MonoVertexMetrics, error) {
	return nil, nil
}
func (f *fakeMonoClient) GetMonoVertexPodReadRates(context.Context) ([]*mvtxdaemon.PodReadRate, error) {
	return f.pods, nil
}
func (f *fakeMonoClient) GetMonoVertexStatus(context.Context) (*mvtxdaemon.MonoVertexStatus, error) {
	return nil, nil
}
func (f *fakeMonoClient) GetMonoVertexErrors(context.Context, string) ([]*mvtxdaemon.ReplicaErrors, error) {
	return nil, nil
}

type fakePipelineClient struct {
	pods []*daemon.PodReadRate
}

func (f *fakePipelineClient) Close() error { return nil }
func (f *fakePipelineClient) IsDrained(context.Context, string) (bool, error) {
	return false, nil
}
func (f *fakePipelineClient) ListPipelineBuffers(context.Context, string) ([]*daemon.BufferInfo, error) {
	return nil, nil
}
func (f *fakePipelineClient) GetPipelineBuffer(context.Context, string, string) (*daemon.BufferInfo, error) {
	return nil, nil
}
func (f *fakePipelineClient) GetVertexMetrics(context.Context, string, string) ([]*daemon.VertexMetrics, error) {
	return nil, nil
}
func (f *fakePipelineClient) GetVertexPodReadRates(context.Context, string, string) ([]*daemon.PodReadRate, error) {
	return f.pods, nil
}
func (f *fakePipelineClient) GetPipelineWatermarks(context.Context, string) ([]*daemon.EdgeWatermark, error) {
	return nil, nil
}
func (f *fakePipelineClient) GetPipelineStatus(context.Context, string) (*daemon.PipelineStatus, error) {
	return nil, nil
}
func (f *fakePipelineClient) GetVertexErrors(context.Context, string, string) ([]*daemon.ReplicaErrors, error) {
	return nil, nil
}
