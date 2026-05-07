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
