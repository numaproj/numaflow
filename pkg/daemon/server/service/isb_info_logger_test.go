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
	"sync"
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
		Name:            name,
		PendingCount:    5,
		AckPendingCount: 2,
		// TotalMessages is the conditional value (LimitsPolicy: NumPending+NumAckPending).
		// StreamMsgs is the unconditional physical stream depth. They are intentionally
		// distinct so tests can verify the logger reads StreamMsgs and not TotalMessages.
		TotalMessages:              7,
		StreamMsgs:                 50,
		StreamFirstSeq:             1,
		StreamLastSeq:              100,
		StreamBytes:                4096,
		ConsumerNumRedelivered:     3,
		ConsumerNumWaiting:         1,
		ConsumerDeliveredStreamSeq: 90,
		ConsumerAckFloorStreamSeq:  85,
		StreamRetention:            "limits",
		StreamDiscard:              "old",
		StreamMaxMsgs:              200,
		StreamMaxBytes:             -1,
		StreamMaxAgeSec:            259200,
		StreamDuplicatesSec:        60,
	}
}

// newPrevState constructs the helpers logISBSnapshot expects for the
// per-buffer prev-tick state. Each test starts with empty maps unless
// it deliberately seeds prev state.
func newPrevState() (map[string]*snapshotState, *sync.Mutex) {
	return make(map[string]*snapshotState), &sync.Mutex{}
}

func TestLogISBSnapshot_Periodic(t *testing.T) {
	// Capture both INFO (snapshot) and WARN (retention drop) entries.
	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	buffers := []*isbsvc.BufferInfo{
		newBufferInfoFixture(v1alpha1.GenerateBufferName("ns", "pl", "cat", 0)),
		newBufferInfoFixture(v1alpha1.GenerateBufferName("ns", "pl", "out", 0)),
	}

	prev, lock := newPrevState()
	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", buffers, prev, lock)

	entries := recorded.All()
	assert.Equal(t, 1, len(entries), "exactly one snapshot log line expected on first tick (no prev state -> no retention-drop log)")
	e := entries[0]
	assert.Equal(t, "ISB snapshot", e.Message)

	fields := e.ContextMap()
	assert.Equal(t, "pl", fields["pipeline"])
	assert.Equal(t, string(v1alpha1.ISBSvcTypeJetStream), fields["isb_svc_type"])
	assert.Equal(t, "periodic", fields["event"])
	assert.EqualValues(t, 2, fields["num_buffers"])

	bufList, ok := fields["buffers"].([]any)
	assert.True(t, ok, "buffers field should be a slice; got %T", fields["buffers"])
	assert.Equal(t, 2, len(bufList))

	first, ok := bufList[0].(map[string]any)
	assert.True(t, ok)
	assert.Contains(t, first, "buffer")
	assert.Contains(t, first, "vertex")
	// stream_msgs must come from StreamMsgs (50), not TotalMessages (7).
	assert.EqualValues(t, 50, first["stream_msgs"])
	assert.EqualValues(t, 5, first["pending"])
	assert.EqualValues(t, 2, first["ack_pending"])
	assert.EqualValues(t, 100, first["stream_last_seq"])
	assert.EqualValues(t, 1, first["stream_first_seq"])
	assert.EqualValues(t, 4096, first["stream_bytes"])
	assert.EqualValues(t, 3, first["num_redelivered"])
	assert.EqualValues(t, 1, first["num_waiting"])
	assert.EqualValues(t, 90, first["delivered_stream_seq"])
	assert.EqualValues(t, 85, first["ack_floor_stream_seq"])
	// inflight = 100 - 85 = 15
	assert.EqualValues(t, 15, first["inflight"])
	// undelivered_lag = 100 - 90 = 10
	assert.EqualValues(t, 10, first["undelivered_lag"])

	// Stream config snapshot fields.
	assert.Equal(t, "limits", first["stream_retention"])
	assert.Equal(t, "old", first["stream_discard"])
	assert.EqualValues(t, 200, first["stream_max_msgs"])
	assert.EqualValues(t, -1, first["stream_max_bytes"])
	assert.EqualValues(t, 259200, first["stream_max_age_sec"])
	assert.EqualValues(t, 60, first["stream_duplicates_sec"])

	// Derived: capacity_pct_msgs = 50/200 = 25; capacity_pct_bytes = -1 (unlimited).
	assert.EqualValues(t, 25, first["capacity_pct_msgs"])
	assert.EqualValues(t, -1, first["capacity_pct_bytes"])
	assert.Equal(t, false, first["at_capacity"])

	// prev state was populated for every buffer for next tick.
	assert.Equal(t, 2, len(prev))
}

func TestLogISBSnapshot_Shutdown(t *testing.T) {
	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	buffers := []*isbsvc.BufferInfo{
		newBufferInfoFixture(v1alpha1.GenerateBufferName("ns", "pl", "cat", 0)),
	}

	prev, lock := newPrevState()
	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "shutdown_snapshot", buffers, prev, lock)

	entries := recorded.All()
	assert.Equal(t, 1, len(entries))
	assert.Equal(t, "shutdown_snapshot", entries[0].ContextMap()["event"])
}

func TestLogISBSnapshot_EmptyBuffers(t *testing.T) {
	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	prev, lock := newPrevState()
	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", nil, prev, lock)

	entries := recorded.All()
	assert.Equal(t, 1, len(entries))
	assert.EqualValues(t, 0, entries[0].ContextMap()["num_buffers"])
}

// Retention-drop is detected when StreamFirstSeq advances past the
// previous tick's ConsumerAckFloorStreamSeq. Simulate two ticks where
// FirstSeq jumps from 1 to 1001 while AckFloor stays at 100 — i.e.
// 900 messages between AckFloor+1 (101) and FirstSeq-1 (1000) were
// silently evicted.
func TestLogISBSnapshot_RetentionDropDetected(t *testing.T) {
	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	bufName := v1alpha1.GenerateBufferName("ns", "pl", "cat", 0)

	// Tick 1: prime prev state. FirstSeq=1, AckFloor=100, LastSeq=2000.
	t1 := newBufferInfoFixture(bufName)
	t1.StreamFirstSeq = 1
	t1.ConsumerAckFloorStreamSeq = 100
	t1.StreamLastSeq = 2000

	prev, lock := newPrevState()
	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", []*isbsvc.BufferInfo{t1}, prev, lock)

	// Tick 2: FirstSeq jumped to 1001 (eviction); AckFloor stayed at 100.
	t2 := newBufferInfoFixture(bufName)
	t2.StreamFirstSeq = 1001
	t2.ConsumerAckFloorStreamSeq = 100 // frozen — chaos kill scenario
	t2.StreamLastSeq = 3000

	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", []*isbsvc.BufferInfo{t2}, prev, lock)

	// Two snapshots + one retention-drop warn.
	entries := recorded.All()
	var dropEntry *observer.LoggedEntry
	for i := range entries {
		if entries[i].Message == "ISB retention drop detected" {
			dropEntry = &entries[i]
			break
		}
	}
	assert.NotNil(t, dropEntry, "ISB retention drop detected log was not emitted")
	if dropEntry == nil {
		return
	}
	assert.Equal(t, zapcore.WarnLevel, dropEntry.Level)
	f := dropEntry.ContextMap()
	assert.Equal(t, bufName, f["buffer"])
	assert.Equal(t, "limits", f["stream_retention"])
	assert.Equal(t, "old", f["stream_discard"])
	assert.EqualValues(t, 1, f["first_seq_before"])
	assert.EqualValues(t, 1001, f["first_seq_after"])
	assert.EqualValues(t, 100, f["ack_floor_at_prev_tick"])
	// evicted_total = 1001 - 1 = 1000.
	assert.EqualValues(t, 1000, f["evicted_total"])
	// evicted_unacked_estimate = 1001 - 100 - 1 = 900.
	assert.EqualValues(t, 900, f["evicted_unacked_estimate"])
}

// First tick after startup must NOT emit a retention-drop log: there
// is no prev state to diff against. Otherwise daemons would scream on
// startup whenever they observe a non-zero FirstSeq.
func TestLogISBSnapshot_NoRetentionDropOnFirstTick(t *testing.T) {
	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	bufName := v1alpha1.GenerateBufferName("ns", "pl", "cat", 0)

	bi := newBufferInfoFixture(bufName)
	// Even with FirstSeq far ahead of AckFloor, no prev = no log.
	bi.StreamFirstSeq = 10000
	bi.ConsumerAckFloorStreamSeq = 0

	prev, lock := newPrevState()
	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", []*isbsvc.BufferInfo{bi}, prev, lock)

	for _, e := range recorded.All() {
		assert.NotEqual(t, "ISB retention drop detected", e.Message,
			"retention-drop log should not fire on first tick")
	}
}

// When FirstSeq advances but only acked messages were evicted (i.e.
// FirstSeq - 1 <= prev AckFloor), no loss occurred — no log.
// Mirrors normal WorkQueue retention behaviour where acked messages
// are auto-deleted.
func TestLogISBSnapshot_NoRetentionDropWhenOnlyAckedEvicted(t *testing.T) {
	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	bufName := v1alpha1.GenerateBufferName("ns", "pl", "cat", 0)

	t1 := newBufferInfoFixture(bufName)
	t1.StreamFirstSeq = 1
	t1.ConsumerAckFloorStreamSeq = 500 // consumer is well ahead

	prev, lock := newPrevState()
	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", []*isbsvc.BufferInfo{t1}, prev, lock)

	// FirstSeq advanced to 400 — still <= prev AckFloor (500), so all evicted seqs were acked.
	t2 := newBufferInfoFixture(bufName)
	t2.StreamFirstSeq = 400
	t2.ConsumerAckFloorStreamSeq = 600

	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", []*isbsvc.BufferInfo{t2}, prev, lock)

	for _, e := range recorded.All() {
		assert.NotEqual(t, "ISB retention drop detected", e.Message,
			"retention-drop log should not fire when only acked seqs were evicted")
	}
}

// at_capacity must flip to true once StreamMsgs reaches the threshold.
func TestLogISBSnapshot_AtCapacityFlag(t *testing.T) {
	core, recorded := observer.New(zapcore.DebugLevel)
	logger := zap.New(core).Sugar()
	ctx := logging.WithLogger(context.Background(), logger)

	pl := newPipelineFixture()
	bufName := v1alpha1.GenerateBufferName("ns", "pl", "cat", 0)

	bi := newBufferInfoFixture(bufName)
	bi.StreamMsgs = 195 // 97.5% of 200
	bi.StreamMaxMsgs = 200

	prev, lock := newPrevState()
	logISBSnapshot(ctx, pl, v1alpha1.ISBSvcTypeJetStream, "periodic", []*isbsvc.BufferInfo{bi}, prev, lock)

	entries := recorded.All()
	assert.Equal(t, 1, len(entries))
	bufList := entries[0].ContextMap()["buffers"].([]any)
	first := bufList[0].(map[string]any)
	assert.Equal(t, true, first["at_capacity"])
	assert.EqualValues(t, 97, first["capacity_pct_msgs"]) // integer division
}

func TestCapacityPct(t *testing.T) {
	assert.EqualValues(t, -1, capacityPct(0, 0))   // unlimited
	assert.EqualValues(t, -1, capacityPct(50, -1)) // unlimited
	assert.EqualValues(t, 0, capacityPct(0, 100))
	assert.EqualValues(t, 50, capacityPct(50, 100))
	assert.EqualValues(t, 100, capacityPct(100, 100))
	// Stream usage can transiently exceed configured limit.
	assert.EqualValues(t, 100, capacityPct(150, 100))
}

func TestAtCapacity(t *testing.T) {
	// Both limits unlimited -> false.
	b := &isbsvc.BufferInfo{StreamMsgs: 1_000_000, StreamMaxMsgs: -1, StreamBytes: 1 << 40, StreamMaxBytes: -1}
	assert.False(t, atCapacity(b))

	// 94% of msgs limit -> below threshold.
	b = &isbsvc.BufferInfo{StreamMsgs: 94, StreamMaxMsgs: 100}
	assert.False(t, atCapacity(b))

	// 95% of msgs limit -> at threshold.
	b = &isbsvc.BufferInfo{StreamMsgs: 95, StreamMaxMsgs: 100}
	assert.True(t, atCapacity(b))

	// Bytes limit drives the flag.
	b = &isbsvc.BufferInfo{StreamMsgs: 0, StreamMaxMsgs: 100, StreamBytes: 96, StreamMaxBytes: 100}
	assert.True(t, atCapacity(b))
}
