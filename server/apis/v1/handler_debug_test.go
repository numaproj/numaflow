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

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/wrapperspb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fakeKube "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
	numaflowfake "github.com/numaproj/numaflow/pkg/client/clientset/versioned/fake"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtdaemonclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
)

// --- test doubles for the daemon clients ---

type fakeDaemonClient struct {
	buffers    []*daemon.BufferInfo
	watermarks []*daemon.EdgeWatermark
	status     *daemon.PipelineStatus
	metrics    []*daemon.VertexMetrics
	errors     []*daemon.ReplicaErrors
	failAll    bool
	// timeoutAll makes every sub-call return context.DeadlineExceeded.
	timeoutAll bool
}

func (f *fakeDaemonClient) Close() error                                    { return nil }
func (f *fakeDaemonClient) IsDrained(context.Context, string) (bool, error) { return false, nil }
func (f *fakeDaemonClient) ListPipelineBuffers(context.Context, string) ([]*daemon.BufferInfo, error) {
	if f.failAll {
		return nil, fmt.Errorf("boom")
	}
	return f.buffers, nil
}
func (f *fakeDaemonClient) GetPipelineBuffer(context.Context, string, string) (*daemon.BufferInfo, error) {
	return nil, nil
}
func (f *fakeDaemonClient) GetVertexMetrics(context.Context, string, string) ([]*daemon.VertexMetrics, error) {
	if f.failAll {
		return nil, fmt.Errorf("boom")
	}
	return f.metrics, nil
}
func (f *fakeDaemonClient) GetPipelineWatermarks(context.Context, string) ([]*daemon.EdgeWatermark, error) {
	if f.failAll {
		return nil, fmt.Errorf("boom")
	}
	return f.watermarks, nil
}
func (f *fakeDaemonClient) GetPipelineStatus(context.Context, string) (*daemon.PipelineStatus, error) {
	if f.timeoutAll {
		return nil, context.DeadlineExceeded
	}
	if f.failAll {
		return nil, fmt.Errorf("boom")
	}
	return f.status, nil
}
func (f *fakeDaemonClient) GetVertexErrors(context.Context, string, string) ([]*daemon.ReplicaErrors, error) {
	if f.timeoutAll {
		return nil, context.DeadlineExceeded
	}
	if f.failAll {
		return nil, fmt.Errorf("boom")
	}
	return f.errors, nil
}

var _ daemonclient.DaemonClient = (*fakeDaemonClient)(nil)

type fakeMvtClient struct {
	metrics *mvtxdaemon.MonoVertexMetrics
}

func (f *fakeMvtClient) Close() error { return nil }
func (f *fakeMvtClient) GetMonoVertexMetrics(context.Context) (*mvtxdaemon.MonoVertexMetrics, error) {
	return f.metrics, nil
}
func (f *fakeMvtClient) GetMonoVertexStatus(context.Context) (*mvtxdaemon.MonoVertexStatus, error) {
	return &mvtxdaemon.MonoVertexStatus{}, nil
}
func (f *fakeMvtClient) GetMonoVertexErrors(context.Context, string) ([]*mvtxdaemon.ReplicaErrors, error) {
	return nil, nil
}

var _ mvtdaemonclient.MonoVertexDaemonClient = (*fakeMvtClient)(nil)

func debugTestContext(params gin.Params) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	c.Params = params
	return c, w
}

func seedClient(t *testing.T, objs ...runtime.Object) dfv1clients.NumaflowV1alpha1Interface {
	t.Helper()
	client := numaflowfake.NewSimpleClientset().NumaflowV1alpha1()
	ctx := context.Background()
	for _, obj := range objs {
		var err error
		switch o := obj.(type) {
		case *dfv1.Pipeline:
			_, err = client.Pipelines(o.Namespace).Create(ctx, o, metav1.CreateOptions{})
		case *dfv1.MonoVertex:
			_, err = client.MonoVertices(o.Namespace).Create(ctx, o, metav1.CreateOptions{})
		default:
			t.Fatalf("unsupported object %T", obj)
		}
		assert.NoError(t, err)
	}
	return client
}

func twoVertexPipeline() *dfv1.Pipeline {
	return &dfv1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "p1"},
		Spec: dfv1.PipelineSpec{
			Vertices: []dfv1.AbstractVertex{
				{Name: "in", Source: &dfv1.Source{}},
				{Name: "out", Sink: &dfv1.Sink{AbstractSink: dfv1.AbstractSink{UDSink: &dfv1.UDSink{}}}},
			},
			Edges: []dfv1.Edge{{From: "in", To: "out"}},
		},
	}
}

func TestHandler_GetPipelineTopology(t *testing.T) {
	h := &handler{numaflowClient: seedClient(t, twoVertexPipeline()), opts: &handlerOptions{}}
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}})

	h.GetPipelineTopology(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var got PipelineTopologyDTO
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got.Vertices, 2)
	assert.Len(t, got.Edges, 1)
	assert.Equal(t, "in", got.Edges[0].From)
	// roles + expected containers
	byName := map[string]TopologyVertexDTO{}
	for _, v := range got.Vertices {
		byName[v.Name] = v
	}
	assert.Equal(t, "source", byName["in"].Type)
	assert.Equal(t, "sink", byName["out"].Type)
	assert.Contains(t, byName["out"].ExpectedContainers, dfv1.CtrUdsink)
	assert.Contains(t, byName["in"].ExpectedContainers, dfv1.CtrMain)
}

func pipelineDaemonHandler(t *testing.T, dc daemonclient.DaemonClient, objs ...runtime.Object) *handler {
	t.Helper()
	cache, _ := lru.New[string, daemonclient.DaemonClient](10)
	cache.Add(pipelineDaemonSvcAddress("ns1", "p1"), dc)
	return &handler{
		numaflowClient:     seedClient(t, objs...),
		kubeClient:         fakeKube.NewSimpleClientset(),
		daemonClientsCache: cache,
		opts:               &handlerOptions{},
		healthChecker:      NewHealthChecker(context.Background()),
	}
}

func TestHandler_GetPipelineBuffers(t *testing.T) {
	dc := &fakeDaemonClient{buffers: []*daemon.BufferInfo{
		{BufferName: "p1-out-0", PendingCount: wrapperspb.Int64(5), TotalMessages: wrapperspb.Int64(9)},
	}}
	h := pipelineDaemonHandler(t, dc, twoVertexPipeline())
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}})

	h.GetPipelineBuffers(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var got []BufferDTO
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got, 1)
	assert.Equal(t, "p1-out-0", got[0].Name)
	assert.Equal(t, int64(5), got[0].Pending)
}

func TestHandler_GetPipelineEdgeWatermarks(t *testing.T) {
	dc := &fakeDaemonClient{watermarks: []*daemon.EdgeWatermark{
		{From: "in", To: "out", Watermarks: []*wrapperspb.Int64Value{wrapperspb.Int64(123)}, IsWatermarkEnabled: wrapperspb.Bool(true)},
	}}
	h := pipelineDaemonHandler(t, dc, twoVertexPipeline())
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}})

	h.GetPipelineEdgeWatermarks(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var got []EdgeWatermarkDTO
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got, 1)
	assert.Equal(t, []int64{123}, got[0].Watermarks)
	assert.True(t, got[0].IsEnabled)
}

func TestHandler_GetMonoVertexThroughputMetrics(t *testing.T) {
	cache, _ := lru.New[string, mvtdaemonclient.MonoVertexDaemonClient](10)
	cache.Add(monoVertexDaemonSvcAddress("ns1", "mv1"), &fakeMvtClient{metrics: &mvtxdaemon.MonoVertexMetrics{
		MonoVertex:      "mv1",
		ProcessingRates: map[string]*wrapperspb.DoubleValue{"1m": wrapperspb.Double(3.5)},
		Pendings:        map[string]*wrapperspb.Int64Value{"1m": wrapperspb.Int64(11)},
	}})
	h := &handler{mvtDaemonClientsCache: cache, opts: &handlerOptions{}}
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "mono-vertex", Value: "mv1"}})

	h.GetMonoVertexThroughputMetrics(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var got MonoVertexMetricsDTO
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, 3.5, got.ProcessingRates["1m"])
	assert.Equal(t, int64(11), got.Pending["1m"])
}

func TestHandler_GetVertexEvents_UsesCRName(t *testing.T) {
	// Event involvedObject uses the Vertex CR name "p1-out", not "out".
	ev := corev1.Event{
		ObjectMeta:     metav1.ObjectMeta{Namespace: "ns1", Name: "e1"},
		InvolvedObject: corev1.ObjectReference{Kind: dfv1.VertexGroupVersionKind.Kind, Name: "p1-out"},
		Type:           "Warning",
		Reason:         "BackOff",
		Message:        "crashloop",
		Count:          3,
		LastTimestamp:  metav1.Now(),
	}
	kube := fakeKube.NewSimpleClientset(&ev)
	h := &handler{kubeClient: kube, opts: &handlerOptions{}}
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}, {Key: "vertex", Value: "out"}})

	h.GetVertexEvents(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var got []K8sEventsResponse
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got, 1)
	assert.Equal(t, "p1-out", got[0].InvolvedObjectName)
	assert.Equal(t, dfv1.VertexGroupVersionKind.Kind, got[0].InvolvedObjectKind)
	assert.Equal(t, "Vertex/p1-out", got[0].Object) // legacy field preserved
	assert.Equal(t, int32(3), got[0].Count)
}

func TestHandler_GetPipelineDebugSnapshot_Composes(t *testing.T) {
	dc := &fakeDaemonClient{
		status:     &daemon.PipelineStatus{Status: "warning", Code: "D2"},
		watermarks: []*daemon.EdgeWatermark{{From: "in", To: "out", Watermarks: []*wrapperspb.Int64Value{wrapperspb.Int64(100)}}},
		buffers:    []*daemon.BufferInfo{{BufferName: "p1-out-0", PendingCount: wrapperspb.Int64(7)}},
		metrics:    []*daemon.VertexMetrics{{Pendings: map[string]*wrapperspb.Int64Value{"1m": wrapperspb.Int64(7)}}},
		errors:     []*daemon.ReplicaErrors{{Replica: "p1-out-0", ContainerErrors: []*daemon.ContainerError{{Container: "udsink", Message: "boom"}}}},
	}
	h := pipelineDaemonHandler(t, dc, twoVertexPipeline())
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}})

	h.GetPipelineDebugSnapshot(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var got DebugSnapshotDTO
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.NotEmpty(t, got.ObservedAt)
	assert.NotNil(t, got.DataHealth.Data)
	assert.Equal(t, "warning", got.DataHealth.Data.Status)
	assert.Len(t, got.Buffers.Data, 1)
	assert.NotNil(t, got.WatermarkLag.Data)
	// both vertices report the same canned error, so both appear
	assert.Len(t, got.RuntimeErrors.Data, 2)
}

func TestHandler_GetPipelineDebugSnapshot_DegradesGracefully(t *testing.T) {
	// Daemon sub-calls all fail; resource health (k8s) still works, and the
	// response is 200 with per-section errors rather than a 502.
	dc := &fakeDaemonClient{failAll: true}
	h := pipelineDaemonHandler(t, dc, twoVertexPipeline())
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}})

	h.GetPipelineDebugSnapshot(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var got DebugSnapshotDTO
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	// daemon-backed sections carry structured errors, not a whole-response 502
	assert.NotNil(t, got.DataHealth.Error)
	assert.Equal(t, SnapshotErrCodeDaemonUnavailable, got.DataHealth.Error.Code)
	assert.NotNil(t, got.Buffers.Error)
	assert.NotNil(t, got.WatermarkLag.Error)
	// resource health (k8s-backed) is attempted independently of the daemon
	assert.NotEmpty(t, got.ResourceHealth.ObservedAt)
}

func TestHandler_GetPipelineDebugSnapshot_TimeoutClassified(t *testing.T) {
	dc := &fakeDaemonClient{timeoutAll: true}
	h := pipelineDaemonHandler(t, dc, twoVertexPipeline())
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}})

	h.GetPipelineDebugSnapshot(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var got DebugSnapshotDTO
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	// deadline errors must classify as timeout, not daemon_unavailable
	assert.NotNil(t, got.DataHealth.Error)
	assert.Equal(t, SnapshotErrCodeTimeout, got.DataHealth.Error.Code)
}

func TestHandler_GetPipelineDebugSnapshot_TotalErrorsCapped(t *testing.T) {
	// Many replicas, each at the per-replica cap, returned for every vertex.
	// Across vertices this far exceeds the total cap; ensure the snapshot never
	// returns more than snapshotMaxTotalErrors errors overall.
	manyReplicas := make([]*daemon.ReplicaErrors, 100)
	for r := range manyReplicas {
		ces := make([]*daemon.ContainerError, snapshotMaxErrorsPerVtx)
		for i := range ces {
			ces[i] = &daemon.ContainerError{Container: "udsink", Message: "boom"}
		}
		manyReplicas[r] = &daemon.ReplicaErrors{Replica: fmt.Sprintf("p1-out-%d", r), ContainerErrors: ces}
	}
	dc := &fakeDaemonClient{errors: manyReplicas}
	h := pipelineDaemonHandler(t, dc, twoVertexPipeline())
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}})

	h.GetPipelineDebugSnapshot(c)

	assert.Equal(t, http.StatusOK, w.Code)
	var got DebugSnapshotDTO
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	total := 0
	for _, ve := range got.RuntimeErrors.Data {
		for _, re := range ve.Errors {
			total += len(re.Errors)
		}
	}
	assert.LessOrEqual(t, total, snapshotMaxTotalErrors)
	assert.Equal(t, snapshotMaxTotalErrors, total) // exactly the budget is consumed
}

func TestHandler_GetVertexEvents_PushesFieldSelector(t *testing.T) {
	kube := fakeKube.NewSimpleClientset()
	var gotFields string
	kube.PrependReactor("list", "events", func(action k8stesting.Action) (bool, runtime.Object, error) {
		la := action.(k8stesting.ListAction)
		gotFields = la.GetListRestrictions().Fields.String()
		return true, &corev1.EventList{}, nil
	})
	h := &handler{kubeClient: kube, opts: &handlerOptions{}}
	c, _ := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}, {Key: "vertex", Value: "out"}})

	h.GetVertexEvents(c)

	// the involvedObject kind+name filter is pushed into the API call rather than
	// loading every namespace event and filtering in memory
	assert.Contains(t, gotFields, "involvedObject.kind=Vertex")
	assert.Contains(t, gotFields, "involvedObject.name=p1-out")
}

func TestHandler_GetPipelineEvents_SignalsScanCeiling(t *testing.T) {
	kube := fakeKube.NewSimpleClientset()
	calls := 0
	kube.PrependReactor("list", "events", func(action k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		items := make([]corev1.Event, int(eventListPageSize))
		for i := range items {
			items[i] = corev1.Event{
				ObjectMeta:     metav1.ObjectMeta{Namespace: "ns1", Name: fmt.Sprintf("event-%d-%d", calls, i)},
				InvolvedObject: corev1.ObjectReference{Kind: dfv1.PipelineGroupVersionKind.Kind, Name: "p1"},
				Type:           "Warning",
				Reason:         "BackOff",
				Message:        "still failing",
				LastTimestamp:  metav1.Now(),
			}
		}
		return true, &corev1.EventList{
			ListMeta: metav1.ListMeta{Continue: "next"},
			Items:    items,
		}, nil
	})
	h := &handler{kubeClient: kube, opts: &handlerOptions{}}
	c, w := debugTestContext(gin.Params{{Key: "namespace", Value: "ns1"}, {Key: "pipeline", Value: "p1"}})

	h.GetPipelineEvents(c)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "true", w.Header().Get("X-Numaflow-Events-Truncated"))
	assert.Equal(t, "true", w.Header().Get("X-Numaflow-Events-Scan-Ceiling-Reached"))
	assert.Equal(t, eventScanCeiling/int(eventListPageSize), calls)
	var got []K8sEventsResponse
	assert.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Len(t, got, int(defaultScopedEventLimit))
}
