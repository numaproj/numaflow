# Daemon-side ISB Info Logging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Emit, from the daemon pod, a structured log line every 10 s — plus a final shutdown snapshot — describing every JetStream ISB stream/consumer state, so operators can debug message-loss and message-stuck conditions from a single low-cost vantage point.

**Architecture:** Extend the daemon-internal `isbsvc.BufferInfo` struct (gRPC proto unchanged) with rich JetStream stream/consumer fields. Populate them from the same `StreamInfo`/`ConsumerInfo` calls already made inside `GetBufferInfo`. Hook a new `logISBSnapshot` helper into the existing health-checker tick loop in `pkg/daemon/server/service/health_status.go`, and add a shutdown branch that emits a final snapshot using a fresh 5 s-timeout context when the parent context is cancelled.

**Tech Stack:** Go (Numaflow daemon), `nats.go` JetStream client, `go.uber.org/zap` for structured logging, `zaptest/observer` for log assertions in tests.

**Spec:** `docs/superpowers/specs/2026-05-07-daemon-isb-info-logging-design.md`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `pkg/isbsvc/interface.go` | modify | Extend internal `BufferInfo` struct with rich JetStream fields. |
| `pkg/isbsvc/jetstream_service.go` | modify | Populate the new fields in `GetBufferInfo` from already-fetched `StreamInfo`/`ConsumerInfo`. |
| `pkg/isbsvc/jetstream_service_test.go` | modify | Extend `TestJetstreamSvc_GetBufferInfo` to assert the new fields. |
| `pkg/daemon/server/service/pipeline_metrics_query.go` | modify | Change `listBuffers` to also return the internal `[]*isbsvc.BufferInfo` slice; thread `isbSvcType` through `NewPipelineMetadataQuery`. |
| `pkg/daemon/server/service/pipeline_metrics_query_test.go` | modify | Update mock and existing test calls for new constructor signature; assert internal slice on `listBuffers`. |
| `pkg/daemon/server/service/health_status.go` | modify | Accept `isbSvcType` in `NewHealthChecker`; in the tick loop call `logISBSnapshot` after a successful `listBuffers`; in the `ctx.Done` branch fire a shutdown snapshot. |
| `pkg/daemon/server/service/health_status_test.go` | modify | Update existing `NewHealthChecker` call sites for the new signature. |
| `pkg/daemon/server/service/isb_info_logger.go` | **create** | Single function `logISBSnapshot` that builds and emits the structured zap log line. |
| `pkg/daemon/server/service/isb_info_logger_test.go` | **create** | Unit tests for `logISBSnapshot` using `zaptest/observer`. |
| `pkg/daemon/server/daemon_server.go` | modify | Pass `ds.isbSvcType` into `NewPipelineMetadataQuery`. |

---

## Task 1: Extend internal `BufferInfo` struct with rich fields

**Why:** The internal struct is what the new logger reads from. Adding the fields here first (with zero values) lets the codebase still compile while we wire population and consumers in subsequent tasks. The struct is daemon-internal — it is never serialised to the gRPC `daemon.BufferInfo` proto, so this is **not** a public API change.

**Files:**
- Modify: `pkg/isbsvc/interface.go:53-58`

- [ ] **Step 1: Apply the struct extension**

Replace the existing `BufferInfo` struct (lines 52-58) with:

```go
// BufferInfo wraps the buffer state information.
//
// The "rich" fields below are populated only by the JetStream
// implementation and are used internally by the daemon for
// structured ISB-state logging. They are NOT serialised onto the
// gRPC `daemon.BufferInfo` proto and so do not constitute a public
// API surface.
type BufferInfo struct {
	Name            string
	PendingCount    int64
	AckPendingCount int64
	TotalMessages   int64

	// Rich JetStream-only fields (zero for non-JetStream backends).
	StreamFirstSeq             int64
	StreamLastSeq              int64
	StreamBytes                int64
	ConsumerNumRedelivered     int64
	ConsumerNumWaiting         int64
	ConsumerDeliveredStreamSeq int64
	ConsumerAckFloorStreamSeq  int64
}
```

- [ ] **Step 2: Verify the package still builds**

Run: `go build ./pkg/isbsvc/...`
Expected: clean build, no errors.

- [ ] **Step 3: Verify existing tests still pass**

Run: `go test ./pkg/isbsvc/... -run . -timeout 60s`
Expected: PASS (existing `TestJetstreamSvc_GetBufferInfo` already asserts only the original three fields, so it continues to pass.)

- [ ] **Step 4: Commit**

```bash
git add pkg/isbsvc/interface.go
git commit -m "feat(isbsvc): add rich JetStream fields to internal BufferInfo

Daemon-internal only; gRPC daemon.BufferInfo proto is unchanged."
```

---

## Task 2: Populate the rich fields in `GetBufferInfo`

**Why:** The fields exist now but are zero. This task makes the JetStream service populate them from the *same* `StreamInfo` / `ConsumerInfo` calls already made — no extra NATS round-trips.

**Files:**
- Modify: `pkg/isbsvc/jetstream_service.go:326-347`
- Modify: `pkg/isbsvc/jetstream_service_test.go:44-81`

- [ ] **Step 1: Update the failing test first**

Edit `pkg/isbsvc/jetstream_service_test.go:44-81`. Replace the body of `TestJetstreamSvc_GetBufferInfo` with the version below. The new assertions for `StreamFirstSeq`, `StreamLastSeq`, `StreamBytes`, etc. will fail until Step 3 lands.

```go
func TestJetstreamSvc_GetBufferInfo(t *testing.T) {
	ctx := context.Background()
	s := test.RunJetStreamServer(t)
	defer test.ShutdownJetStreamServer(t, s)

	conn, err := nats.Connect(s.ClientURL())
	assert.NoError(t, err)
	defer conn.Close()

	client := nats2.NewTestClient(t, s.ClientURL())
	defer client.Close()

	jsCtx, err := client.JetStreamContext()
	assert.NoError(t, err)

	// add stream
	_, err = jsCtx.AddStream(&nats.StreamConfig{
		Name:     "test-buffer",
		Subjects: []string{"test-buffer"},
	})
	assert.NoError(t, err)

	// add consumer
	_, err = jsCtx.AddConsumer("test-buffer", &nats.ConsumerConfig{
		Durable:       "test-buffer",
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: "test-buffer",
	})
	assert.NoError(t, err)

	isbSvc, err := NewISBJetStreamSvc(client)
	assert.NoError(t, err)

	buffer := "test-buffer"

	// Empty-state baseline.
	info, err := isbSvc.GetBufferInfo(ctx, buffer)
	assert.NoError(t, err)
	assert.Equal(t, buffer, info.Name)
	assert.Equal(t, int64(0), info.PendingCount)
	assert.Equal(t, int64(0), info.AckPendingCount)
	assert.Equal(t, int64(0), info.TotalMessages)
	assert.Equal(t, int64(0), info.StreamFirstSeq)
	assert.Equal(t, int64(0), info.StreamLastSeq)
	assert.Equal(t, int64(0), info.StreamBytes)
	assert.Equal(t, int64(0), info.ConsumerNumRedelivered)
	assert.Equal(t, int64(0), info.ConsumerNumWaiting)
	assert.Equal(t, int64(0), info.ConsumerDeliveredStreamSeq)
	assert.Equal(t, int64(0), info.ConsumerAckFloorStreamSeq)

	// Publish 3 messages so stream state has content.
	for i := 0; i < 3; i++ {
		_, err = jsCtx.Publish("test-buffer", []byte("hello"))
		assert.NoError(t, err)
	}

	info, err = isbSvc.GetBufferInfo(ctx, buffer)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), info.StreamFirstSeq)
	assert.Equal(t, int64(3), info.StreamLastSeq)
	assert.Greater(t, info.StreamBytes, int64(0))
	// Nothing has been delivered/acked yet.
	assert.Equal(t, int64(3), info.PendingCount)
	assert.Equal(t, int64(0), info.AckPendingCount)
	assert.Equal(t, int64(0), info.ConsumerDeliveredStreamSeq)
	assert.Equal(t, int64(0), info.ConsumerAckFloorStreamSeq)
	assert.Equal(t, int64(0), info.ConsumerNumRedelivered)
}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `go test ./pkg/isbsvc/... -run TestJetstreamSvc_GetBufferInfo -v -timeout 60s`
Expected: FAIL on the new assertions (e.g. `assert.Equal(int64(1), info.StreamFirstSeq)` — actual will be 0).

- [ ] **Step 3: Implement field population**

Edit `pkg/isbsvc/jetstream_service.go:326-347`. Replace `GetBufferInfo` with:

```go
func (jss *jetStreamSvc) GetBufferInfo(ctx context.Context, buffer string) (*BufferInfo, error) {
	streamName := JetStreamName(buffer)
	stream, err := jss.js.StreamInfo(streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get information of stream %q", streamName)
	}
	consumer, err := jss.js.ConsumerInfo(streamName, streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer information of stream %q", streamName)
	}
	totalMessages := int64(stream.State.Msgs)
	if stream.Config.Retention == nats.LimitsPolicy {
		totalMessages = int64(consumer.NumPending) + int64(consumer.NumAckPending)
	}
	bufferInfo := &BufferInfo{
		Name:            buffer,
		PendingCount:    int64(consumer.NumPending),
		AckPendingCount: int64(consumer.NumAckPending),
		TotalMessages:   totalMessages,

		StreamFirstSeq:             int64(stream.State.FirstSeq),
		StreamLastSeq:              int64(stream.State.LastSeq),
		StreamBytes:                int64(stream.State.Bytes),
		ConsumerNumRedelivered:     int64(consumer.NumRedelivered),
		ConsumerNumWaiting:         int64(consumer.NumWaiting),
		ConsumerDeliveredStreamSeq: int64(consumer.Delivered.Stream),
		ConsumerAckFloorStreamSeq:  int64(consumer.AckFloor.Stream),
	}
	return bufferInfo, nil
}
```

- [ ] **Step 4: Run the test to confirm it passes**

Run: `go test ./pkg/isbsvc/... -run TestJetstreamSvc_GetBufferInfo -v -timeout 60s`
Expected: PASS.

- [ ] **Step 5: Run the wider isbsvc test suite to confirm no regression**

Run: `go test ./pkg/isbsvc/... -timeout 60s`
Expected: PASS for all tests in the package.

- [ ] **Step 6: Commit**

```bash
git add pkg/isbsvc/jetstream_service.go pkg/isbsvc/jetstream_service_test.go
git commit -m "feat(isbsvc): populate rich JetStream fields on BufferInfo"
```

---

## Task 3: Refactor `listBuffers` to also return the internal slice

**Why:** The new logger needs the rich `[]*isbsvc.BufferInfo` slice. Currently `listBuffers` only returns the gRPC slice. We change the signature to return both — the existing two callers update accordingly. No extra NATS calls; we just keep the value we already had instead of throwing it away.

**Files:**
- Modify: `pkg/daemon/server/service/pipeline_metrics_query.go:236-271, 98-104, 173-182, 226-228`
- Modify: `pkg/daemon/server/service/health_status.go:226-256`
- Modify: `pkg/daemon/server/service/pipeline_metrics_query_test.go:225-234`

- [ ] **Step 1: Update the test first**

Edit `pkg/daemon/server/service/pipeline_metrics_query_test.go:197-234`. Replace `TestListBuffers` body's tail with the version below (everything after `pipelineMetricsQueryService, err := ...`):

```go
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(context.Background(), ms, pipeline, nil, nil, v1alpha1.ISBSvcTypeJetStream)
	assert.NoError(t, err)

	req := &daemon.ListBuffersRequest{Pipeline: pipelineName}

	resp, err := pipelineMetricsQueryService.ListBuffers(context.Background(), req)
	assert.NoError(t, err)
	assert.Equal(t, len(resp.Buffers), 2)

	// Direct call to the internal helper exposes the rich slice.
	gRPCResp, internal, err := listBuffers(context.Background(), pipeline, ms)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(gRPCResp.Buffers))
	assert.Equal(t, 2, len(internal))
	assert.Equal(t, gRPCResp.Buffers[0].BufferName, internal[0].Name)
}
```

Update the other two `NewPipelineMetadataQuery` call sites in the same file (lines 117 and 185) to pass the new `isbSvcType` argument. The full replacement for line 117:

```go
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(context.Background(), client, pipeline, &mockRater_TestGetVertexMetrics{}, nil, v1alpha1.ISBSvcTypeJetStream)
```

And for line 185:

```go
	pipelineMetricsQueryService, err := NewPipelineMetadataQuery(context.Background(), ms, pipeline, nil, nil, v1alpha1.ISBSvcTypeJetStream)
```

- [ ] **Step 2: Run the test — expect a compile error**

Run: `go test ./pkg/daemon/server/service/... -run TestListBuffers -v -timeout 60s`
Expected: COMPILE ERROR (`listBuffers` returns 2 values not 3; `NewPipelineMetadataQuery` signature mismatch).

- [ ] **Step 3: Update the `listBuffers` helper signature and body**

Edit `pkg/daemon/server/service/pipeline_metrics_query.go`. Replace the existing `listBuffers` function (lines 234-271) with:

```go
// listBuffers returns the list of ISB buffers for the pipeline and their information.
//
// It returns both the gRPC-shaped response (used by the public ListBuffers RPC
// and the health checker's buffer-usage logic) and the slice of internal
// isbsvc.BufferInfo values. The latter carries rich JetStream-only fields
// that are needed by the daemon's structured ISB-state logger but are
// deliberately not exposed on the gRPC proto.
func listBuffers(ctx context.Context, pipeline *v1alpha1.Pipeline, isbSvcClient isbsvc.ISBService) (*daemon.ListBuffersResponse, []*isbsvc.BufferInfo, error) {
	log := logging.FromContext(ctx)
	resp := new(daemon.ListBuffersResponse)

	buffers := []*daemon.BufferInfo{}
	internal := []*isbsvc.BufferInfo{}
	for _, buffer := range pipeline.GetAllBuffers() {
		bufferInfo, err := isbSvcClient.GetBufferInfo(ctx, buffer)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get information of buffer %q", buffer)
		}
		log.Debugf("Buffer %s has bufferInfo %+v", buffer, bufferInfo)
		v := pipeline.FindVertexWithBuffer(buffer)
		if v == nil {
			return nil, nil, fmt.Errorf("unexpected error, buffer %q not found from the pipeline", buffer)
		}
		bufferLength, bufferUsageLimit := getBufferLimits(pipeline, *v)
		usage := float64(bufferInfo.TotalMessages) / float64(bufferLength)
		if x := (float64(bufferInfo.PendingCount) + float64(bufferInfo.AckPendingCount)) / float64(bufferLength); x < usage {
			usage = x
		}
		b := &daemon.BufferInfo{
			Pipeline:         pipeline.Name,
			BufferName:       buffer,
			PendingCount:     wrapperspb.Int64(bufferInfo.PendingCount),
			AckPendingCount:  wrapperspb.Int64(bufferInfo.AckPendingCount),
			TotalMessages:    wrapperspb.Int64(bufferInfo.TotalMessages),
			BufferLength:     wrapperspb.Int64(bufferLength),
			BufferUsageLimit: wrapperspb.Double(bufferUsageLimit),
			BufferUsage:      wrapperspb.Double(usage),
			IsFull:           wrapperspb.Bool(usage >= bufferUsageLimit),
		}
		buffers = append(buffers, b)
		internal = append(internal, bufferInfo)
	}
	resp.Buffers = buffers
	return resp, internal, nil
}
```

- [ ] **Step 4: Update the gRPC `ListBuffers` handler**

Edit `pkg/daemon/server/service/pipeline_metrics_query.go:97-104`. Replace `ListBuffers` with:

```go
// ListBuffers is used to obtain the all the edge buffers information of a pipeline
func (ps *PipelineMetadataQuery) ListBuffers(ctx context.Context, req *daemon.ListBuffersRequest) (*daemon.ListBuffersResponse, error) {
	resp, _, err := listBuffers(ctx, ps.pipeline, ps.isbSvcClient)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
```

- [ ] **Step 5: Update the health checker caller**

Edit `pkg/daemon/server/service/health_status.go:226-256`. Replace `getPipelineVertexDataCriticality` with the version below — note the `_` to discard the internal slice for now (Task 5 will use it).

```go
func (hc *HealthChecker) getPipelineVertexDataCriticality(ctx context.Context) ([]*vertexState, error) {
	// Fetch the buffer information for the pipeline
	buffers, _, err := listBuffers(ctx, hc.pipeline, hc.isbSvcClient)
	if err != nil {
		return nil, err
	}
	// update the usage timeline for all the ISBs used in the pipeline
	hc.updateUsageTimeline(buffers.Buffers)

	var vertexState []*vertexState

	// iterate over the timeline data for each buffer and calculate the exponential weighted mean average
	// for the last HEALTH_WINDOW_SIZE buffer usage entries
	for bufferName := range hc.timelineData {
		// Extract the buffer usage of the timeline
		var bufferUsage []float64
		for _, entry := range hc.timelineData[bufferName].Items() {
			bufferUsage = append(bufferUsage, entry.BufferUsage)
		}
		// calculate the average buffer usage over the last HEALTH_WINDOW_SIZE seconds
		ewmaBufferUsage := calculateEWMAUsage(bufferUsage)
		// assign the state to the vertex based on the average buffer usage
		// Look back is enabled for the critical state
		currentState := assignStateToTimeline(ewmaBufferUsage, enableCriticalLookBack)
		// create a new vertex state object
		currentVertexState := newVertexState(bufferName, currentState)
		// add the vertex state to the list of vertex states
		vertexState = append(vertexState, currentVertexState)
	}
	return vertexState, nil
}
```

- [ ] **Step 6: Update `NewPipelineMetadataQuery` and `PipelineMetadataQuery` to carry `isbSvcType`**

Edit `pkg/daemon/server/service/pipeline_metrics_query.go`. Replace the `PipelineMetadataQuery` struct (lines 45-55) and `NewPipelineMetadataQuery` (lines 57-83) with:

```go
// PipelineMetadataQuery has the metadata required for the pipeline queries
type PipelineMetadataQuery struct {
	daemon.UnimplementedDaemonServiceServer
	isbSvcClient         isbsvc.ISBService
	isbSvcType           v1alpha1.ISBSvcType
	pipeline             *v1alpha1.Pipeline
	httpClient           metricsHttpClient
	watermarkService     *watermark.HTTPWatermarkService
	rater                rater.Ratable
	pipelineRuntimeCache runtimeinfo.PipelineRuntimeCache
	healthChecker        *HealthChecker
}

// NewPipelineMetadataQuery returns a new instance of pipelineMetadataQuery
func NewPipelineMetadataQuery(
	ctx context.Context,
	isbSvcClient isbsvc.ISBService,
	pipeline *v1alpha1.Pipeline,
	rater rater.Ratable,
	pipelineRuntimeCache runtimeinfo.PipelineRuntimeCache,
	isbSvcType v1alpha1.ISBSvcType) (*PipelineMetadataQuery, error) {

	// Create HTTP watermark service
	watermarkService := watermark.NewHTTPWatermarkService(ctx, pipeline)

	ps := PipelineMetadataQuery{
		isbSvcClient: isbSvcClient,
		isbSvcType:   isbSvcType,
		pipeline:     pipeline,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		},
		watermarkService:     watermarkService,
		rater:                rater,
		healthChecker:        NewHealthChecker(pipeline, isbSvcClient, isbSvcType),
		pipelineRuntimeCache: pipelineRuntimeCache,
	}
	return &ps, nil
}
```

(Note: `NewHealthChecker` will accept the third argument starting in Task 5; for now, the line as written will fail to compile until Task 5 lands. We accept the temporary breakage and apply Task 5 immediately.)

- [ ] **Step 7: Update `daemon_server.go` caller**

Edit `pkg/daemon/server/daemon_server.go:193`. Replace:

```go
	pipelineMetadataQuery, err := service.NewPipelineMetadataQuery(ctx, isbSvcClient, ds.pipeline, rater, pipelineRuntimeCache)
```

with:

```go
	pipelineMetadataQuery, err := service.NewPipelineMetadataQuery(ctx, isbSvcClient, ds.pipeline, rater, pipelineRuntimeCache, ds.isbSvcType)
```

- [ ] **Step 8: Defer commit until Task 5**

The package will not build between Tasks 3 and 5 because Task 5 changes the `NewHealthChecker` signature referenced by Task 3 Step 6. **Do not commit yet.** Proceed to Task 4 (which is independent — new file) and Task 5 (which closes the loop). Tasks 3, 4, 5 are committed together.

---

## Task 4: Create the `logISBSnapshot` helper

**Why:** Self-contained new function with no external state. We TDD it independently before wiring it in.

**Files:**
- Create: `pkg/daemon/server/service/isb_info_logger.go`
- Create: `pkg/daemon/server/service/isb_info_logger_test.go`

- [ ] **Step 1: Write the failing test**

Create `pkg/daemon/server/service/isb_info_logger_test.go`:

```go
/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func newPipelineFixture() *v1alpha1.Pipeline {
	return &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pl",
			Namespace: "ns",
		},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{
				{Name: "in", Source: &v1alpha1.Source{}},
				{Name: "cat", UDF: &v1alpha1.UDF{}},
				{Name: "out", Sink: &v1alpha1.Sink{}},
			},
			Edges: []v1alpha1.Edge{
				{From: "in", To: "cat"},
				{From: "cat", To: "out"},
			},
		},
	}
}

func newBufferInfoFixture(name string) *isbsvc.BufferInfo {
	return &isbsvc.BufferInfo{
		Name:                       name,
		PendingCount:               5,
		AckPendingCount:            2,
		TotalMessages:              7,
		StreamFirstSeq:             1,
		StreamLastSeq:               100,
		StreamBytes:                4096,
		ConsumerNumRedelivered:     3,
		ConsumerNumWaiting:         1,
		ConsumerDeliveredStreamSeq: 90,
		ConsumerAckFloorStreamSeq:  85,
	}
}

func TestLogISBSnapshot_Periodic(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	buffers := []*isbsvc.BufferInfo{
		newBufferInfoFixture(v1alpha1.GenerateBufferName("ns", "pl", "cat", 0)),
		newBufferInfoFixture(v1alpha1.GenerateBufferName("ns", "pl", "out", 0)),
	}

	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", buffers)

	entries := recorded.All()
	assert.Equal(t, 1, len(entries), "exactly one snapshot log line expected per call")
	e := entries[0]
	assert.Equal(t, "ISB snapshot", e.Message)

	fields := e.ContextMap()
	assert.Equal(t, "pl", fields["pipeline"])
	assert.Equal(t, string(v1alpha1.ISBSvcTypeJetStream), fields["isb_svc_type"])
	assert.Equal(t, "periodic", fields["event"])
	// num_buffers is logged as int -> serialised as int64 by observer.
	assert.EqualValues(t, 2, fields["num_buffers"])

	bufList, ok := fields["buffers"].([]any)
	assert.True(t, ok, "buffers field should be a slice; got %T", fields["buffers"])
	assert.Equal(t, 2, len(bufList))

	first, ok := bufList[0].(map[string]any)
	assert.True(t, ok)
	assert.Contains(t, first, "buffer")
	assert.Contains(t, first, "vertex")
	assert.EqualValues(t, 5, first["pending"])
	assert.EqualValues(t, 2, first["ack_pending"])
	assert.EqualValues(t, 100, first["stream_last_seq"])
	assert.EqualValues(t, 1, first["stream_first_seq"])
	assert.EqualValues(t, 4096, first["stream_bytes"])
	assert.EqualValues(t, 3, first["num_redelivered"])
	assert.EqualValues(t, 1, first["num_waiting"])
	assert.EqualValues(t, 90, first["delivered_stream_seq"])
	assert.EqualValues(t, 85, first["ack_floor_stream_seq"])
	// inflight = stream_last_seq - ack_floor_stream_seq = 100 - 85 = 15
	assert.EqualValues(t, 15, first["inflight"])
}

func TestLogISBSnapshot_Shutdown(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	buffers := []*isbsvc.BufferInfo{
		newBufferInfoFixture(v1alpha1.GenerateBufferName("ns", "pl", "cat", 0)),
	}

	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "shutdown_snapshot", buffers)

	entries := recorded.All()
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, "shutdown_snapshot", entries[0].ContextMap()["event"])
}

func TestLogISBSnapshot_EmptyBuffers(t *testing.T) {
	core, recorded := observer.New(zapcore.InfoLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", nil)

	entries := recorded.All()
	assert.Equal(t, 1, len(entries))
	assert.EqualValues(t, 0, entries[0].ContextMap()["num_buffers"])
}
```

- [ ] **Step 2: Run the test to confirm it fails**

Run: `go test ./pkg/daemon/server/service/... -run TestLogISBSnapshot -v -timeout 60s`
Expected: COMPILE ERROR (`undefined: logISBSnapshot`).

- [ ] **Step 3: Implement `logISBSnapshot`**

Create `pkg/daemon/server/service/isb_info_logger.go`:

```go
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

package service

import (
	"context"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// logISBSnapshot emits a single structured INFO log line describing the
// state of every ISB stream/consumer used by the pipeline. It is invoked
// from the daemon's health-checker tick loop on every periodic tick and
// once more (with event="shutdown_snapshot") when the daemon is shutting
// down. The fields and naming are documented in
// docs/superpowers/specs/2026-05-07-daemon-isb-info-logging-design.md.
func logISBSnapshot(
	ctx context.Context,
	pipeline *v1alpha1.Pipeline,
	isbSvcType v1alpha1.ISBSvcType,
	event string,
	buffers []*isbsvc.BufferInfo,
) {
	log := logging.FromContext(ctx)

	bufferEntries := make([]map[string]any, 0, len(buffers))
	for _, b := range buffers {
		vertexName := ""
		if v := pipeline.FindVertexWithBuffer(b.Name); v != nil {
			vertexName = v.Name
		}
		inflight := b.StreamLastSeq - b.ConsumerAckFloorStreamSeq
		if inflight < 0 {
			inflight = 0
		}
		bufferEntries = append(bufferEntries, map[string]any{
			"buffer":               b.Name,
			"vertex":               vertexName,
			"stream_msgs":          b.TotalMessages,
			"stream_first_seq":     b.StreamFirstSeq,
			"stream_last_seq":      b.StreamLastSeq,
			"stream_bytes":         b.StreamBytes,
			"pending":              b.PendingCount,
			"ack_pending":          b.AckPendingCount,
			"num_redelivered":      b.ConsumerNumRedelivered,
			"num_waiting":          b.ConsumerNumWaiting,
			"delivered_stream_seq": b.ConsumerDeliveredStreamSeq,
			"ack_floor_stream_seq": b.ConsumerAckFloorStreamSeq,
			"inflight":             inflight,
		})
	}

	log.Infow("ISB snapshot",
		zap.String("pipeline", pipeline.Name),
		zap.String("isb_svc_type", string(isbSvcType)),
		zap.String("event", event),
		zap.Int("num_buffers", len(buffers)),
		zap.Any("buffers", bufferEntries),
	)
}
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `go test ./pkg/daemon/server/service/... -run TestLogISBSnapshot -v -timeout 60s`
Expected: PASS for all three sub-tests.

- [ ] **Step 5: Defer commit until Task 5**

Same reason as Task 3 Step 8 — package won't fully build until Task 5 lands.

---

## Task 5: Wire the periodic logger into the health-checker tick

**Why:** With the helper from Task 4 and the `listBuffers` refactor from Task 3, this task is the actual integration: thread `isbSvcType` into `HealthChecker`, capture the rich slice, and call `logISBSnapshot` after each successful tick.

**Files:**
- Modify: `pkg/daemon/server/service/health_status.go:118-256`
- Modify: `pkg/daemon/server/service/health_status_test.go:68-100`

- [ ] **Step 1: Update `NewHealthChecker` signature and struct**

Edit `pkg/daemon/server/service/health_status.go:118-138`. Replace the `HealthChecker` struct and `NewHealthChecker` with:

```go
// HealthChecker is the struct type for health checker.
type HealthChecker struct {
	// Add a field for the health status.
	currentDataStatus *dataHealthResponse
	isbSvcClient      isbsvc.ISBService
	isbSvcType        v1alpha1.ISBSvcType
	pipeline          *v1alpha1.Pipeline
	timelineData      map[string]*sharedqueue.OverflowQueue[*timelineEntry]
	statusLock        *sync.RWMutex
}

// NewHealthChecker creates a new object HealthChecker struct type.
func NewHealthChecker(pipeline *v1alpha1.Pipeline, isbSvcClient isbsvc.ISBService, isbSvcType v1alpha1.ISBSvcType) *HealthChecker {
	// Return a new HealthChecker struct instance.
	return &HealthChecker{
		currentDataStatus: defaultDataHealthResponse,
		isbSvcClient:      isbSvcClient,
		isbSvcType:        isbSvcType,
		pipeline:          pipeline,
		timelineData:      make(map[string]*sharedqueue.OverflowQueue[*timelineEntry]),
		statusLock:        &sync.RWMutex{},
	}
}
```

- [ ] **Step 2: Capture and forward the rich slice in `getPipelineVertexDataCriticality`**

Edit `pkg/daemon/server/service/health_status.go`. Replace the function body (the version edited in Task 3 Step 5) so it returns the internal slice along with vertex states. The new signature is:

```go
func (hc *HealthChecker) getPipelineVertexDataCriticality(ctx context.Context) ([]*vertexState, []*isbsvc.BufferInfo, error) {
	// Fetch the buffer information for the pipeline
	buffers, internal, err := listBuffers(ctx, hc.pipeline, hc.isbSvcClient)
	if err != nil {
		return nil, nil, err
	}
	// update the usage timeline for all the ISBs used in the pipeline
	hc.updateUsageTimeline(buffers.Buffers)

	var vertexState []*vertexState

	for bufferName := range hc.timelineData {
		var bufferUsage []float64
		for _, entry := range hc.timelineData[bufferName].Items() {
			bufferUsage = append(bufferUsage, entry.BufferUsage)
		}
		ewmaBufferUsage := calculateEWMAUsage(bufferUsage)
		currentState := assignStateToTimeline(ewmaBufferUsage, enableCriticalLookBack)
		currentVertexState := newVertexState(bufferName, currentState)
		vertexState = append(vertexState, currentVertexState)
	}
	return vertexState, internal, nil
}
```

- [ ] **Step 3: Update `startHealthCheck` to call the logger and to handle shutdown**

Edit `pkg/daemon/server/service/health_status.go:178-209`. Replace `startHealthCheck` with:

```go
// startHealthCheck starts the health check for the pipeline.
// The ticks are generated at the interval of healthTimeStep.
//
// On every successful tick, after the health computation, the same
// already-fetched buffer state is also handed off to logISBSnapshot
// (event="periodic") so the daemon emits one structured ISB-state
// log line per tick. When the parent context is cancelled (daemon
// shutdown), one final logISBSnapshot is emitted with
// event="shutdown_snapshot", using a fresh 5s-timeout context so it
// can complete even though the parent ctx is already done.
func (hc *HealthChecker) startHealthCheck(ctx context.Context) {
	logger := logging.FromContext(ctx)
	ticker := time.NewTicker(healthTimeStep)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			criticality, internal, err := hc.getPipelineVertexDataCriticality(ctx)
			logger.Debugw("Health check", zap.Any("criticality", criticality))
			if err != nil {
				logger.Errorw("Failed to vertex data criticality", zap.Error(err))
				hc.setCurrentHealth(defaultDataHealthResponse)
			} else {
				pipelineState := convertVertexStateToPipelineState(criticality)
				hc.setCurrentHealth(pipelineState)
				logISBSnapshot(ctx, hc.pipeline, hc.isbSvcType, "periodic", internal)
			}
		case <-ctx.Done():
			hc.emitShutdownSnapshot()
			return
		}
	}
}

// emitShutdownSnapshot best-effort emits a final "ISB snapshot" log
// line tagged event="shutdown_snapshot" before the daemon process
// exits. It uses a fresh 5s-timeout context (the parent context is
// already cancelled by SIGTERM at this point). On any error the
// snapshot is skipped with a warning — shutdown must never block on
// this.
func (hc *HealthChecker) emitShutdownSnapshot() {
	logger := logging.NewLogger().Named("isb-info-logger").With("pipeline", hc.pipeline.Name)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = logging.WithLogger(ctx, logger)

	_, internal, err := listBuffers(ctx, hc.pipeline, hc.isbSvcClient)
	if err != nil {
		logger.Warnw("ISB snapshot at shutdown unavailable", zap.Error(err))
		return
	}
	logISBSnapshot(ctx, hc.pipeline, hc.isbSvcType, "shutdown_snapshot", internal)
}
```

- [ ] **Step 4: Update `health_status_test.go` for the new constructor**

Edit `pkg/daemon/server/service/health_status_test.go:98`. Replace:

```go
			hc := NewHealthChecker(tt.pipeline, tt.isbSvc)
```

with:

```go
			hc := NewHealthChecker(tt.pipeline, tt.isbSvc, v1alpha1.ISBSvcTypeJetStream)
```

If `v1alpha1` is not yet imported in `health_status_test.go`, add the import:

```go
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
```

- [ ] **Step 5: Build the whole package**

Run: `go build ./...`
Expected: clean build.

- [ ] **Step 6: Run all touched test packages**

Run: `go test ./pkg/isbsvc/... ./pkg/daemon/server/service/... -timeout 120s`
Expected: PASS — including new `TestLogISBSnapshot_*`, updated `TestJetstreamSvc_GetBufferInfo`, updated `TestListBuffers`, and `TestNewHealthChecker`.

- [ ] **Step 7: Commit Tasks 3, 4 and 5 together**

```bash
git add pkg/isbsvc/interface.go \
        pkg/isbsvc/jetstream_service.go \
        pkg/isbsvc/jetstream_service_test.go \
        pkg/daemon/server/daemon_server.go \
        pkg/daemon/server/service/pipeline_metrics_query.go \
        pkg/daemon/server/service/pipeline_metrics_query_test.go \
        pkg/daemon/server/service/health_status.go \
        pkg/daemon/server/service/health_status_test.go \
        pkg/daemon/server/service/isb_info_logger.go \
        pkg/daemon/server/service/isb_info_logger_test.go
git commit -m "feat(daemon): periodic and shutdown ISB-state structured logging

Adds a single 'ISB snapshot' INFO log line, emitted every 10s and
once more on daemon shutdown, listing per-buffer JetStream stream
and consumer state. Designed to support message-loss debugging
without per-pod logging cost. Spec:
docs/superpowers/specs/2026-05-07-daemon-isb-info-logging-design.md"
```

---

## Task 6: Whole-repo verification

**Why:** Confirm nothing outside the touched packages regressed (e.g. callers of `NewPipelineMetadataQuery` we missed).

**Files:** none (verification only).

- [ ] **Step 1: Repo-wide build**

Run: `go build ./...`
Expected: clean.

- [ ] **Step 2: Repo-wide unit tests**

Run: `make test`
Expected: PASS. (Reminder: `make test` runs Go unit tests only, with `-race -short -timeout 60s`.)

- [ ] **Step 3: Lint**

Run: `make lint`
Expected: clean.

- [ ] **Step 4: Inspect a sample log line manually**

Run a local pipeline as you normally would (e.g. via `make start` and applying a sample pipeline). Tail the daemon pod logs:

```bash
kubectl -n numaflow-system logs -f $(kubectl -n numaflow-system get pod -l 'numaflow.numaproj.io/component=daemon' -o name | head -n1) | grep "ISB snapshot"
```

Expected: one line every 10 s with `event=periodic` and the schema fields. Delete the pipeline and confirm one final line with `event=shutdown_snapshot` is logged before the daemon pod terminates.

- [ ] **Step 5: If verification reveals any issue, fix it and add a follow-up commit. Otherwise, the plan is complete.**

---

## Self-Review Checklist (already applied)

- **Spec coverage:** Every section of the design doc (architecture, log line schema, error handling for periodic + shutdown, testing scope, out-of-scope items) maps to a task above. The "Acceptance" criteria in the spec are covered by Tasks 2, 4, 5, 6.
- **Placeholders:** None. Every step has either exact code or an exact command + expected output.
- **Type consistency:** All numeric fields on `BufferInfo` are `int64`. The logger's `inflight` is computed as `int64`. Constructor signatures referenced across tasks (`NewPipelineMetadataQuery`, `NewHealthChecker`) match between Tasks 3, 5 and tests.
- **Cross-task coherence:** Tasks 3, 4, 5 are explicitly noted as committing together because the package only fully builds at the end of Task 5.
