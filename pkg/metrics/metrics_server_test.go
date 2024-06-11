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

package metrics

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gavv/httpexpect/v2"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"k8s.io/utils/ptr"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
)

func Test_StartMetricsServer(t *testing.T) {
	t.SkipNow() // flaky
	ms := NewMetricsServer(&dfv1.Vertex{})
	s, err := ms.Start(context.TODO())
	assert.NoError(t, err)
	assert.NotNil(t, s)
	e := httpexpect.WithConfig(httpexpect.Config{
		BaseURL:  fmt.Sprintf("https://localhost:%d", dfv1.VertexMetricsPort),
		Reporter: httpexpect.NewRequireReporter(t),
		Client: &http.Client{
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
		},
	})
	e.GET("/ready").WithMaxRetries(3).WithRetryDelay(time.Second, 3*time.Second).Expect().Status(204)
	e.GET("/metrics").WithMaxRetries(3).WithRetryDelay(time.Second, 3*time.Second).Expect().Status(200)
	err = s(context.TODO())
	assert.NoError(t, err)
}

func Test_MetricsServer_WithLagReaders(t *testing.T) {
	mockReader := &mockLagReader{name: "test-reader"}
	ms := NewMetricsServer(&dfv1.Vertex{}, WithLagReaders(map[string]isb.LagReader{
		"test-reader": mockReader,
	}))
	assert.Equal(t, 1, len(ms.lagReaders))
	assert.Equal(t, mockReader, ms.lagReaders["test-reader"])
}

func Test_MetricsServer_WithRefreshInterval(t *testing.T) {
	interval := 10 * time.Second
	ms := NewMetricsServer(&dfv1.Vertex{}, WithRefreshInterval(interval))
	assert.Equal(t, interval, ms.refreshInterval)
}

func Test_MetricsServer_WithLookbackSeconds(t *testing.T) {
	seconds := int64(300)
	ms := NewMetricsServer(&dfv1.Vertex{}, WithLookbackSeconds(seconds))
	assert.Equal(t, seconds, ms.lookbackSeconds)
}

func Test_MetricsServer_WithHealthCheckExecutor(t *testing.T) {
	executed := false
	executor := func() error {
		executed = true
		return nil
	}
	ms := NewMetricsServer(&dfv1.Vertex{}, WithHealthCheckExecutor(executor))
	assert.Equal(t, 1, len(ms.healthCheckExecutors))
	err := ms.healthCheckExecutors[0]()
	assert.NoError(t, err)
	assert.True(t, executed)
}

func Test_MetricsServer_NewMetricsOptions(t *testing.T) {
	vertex := &dfv1.Vertex{
		Spec: dfv1.VertexSpec{
			AbstractVertex: dfv1.AbstractVertex{
				Name: "test-vertex",
				Scale: dfv1.Scale{
					LookbackSeconds: ptr.To[uint32](120),
				},
			},
		},
	}
	healthChecker := &mockHealthChecker{}
	reader := &mockLagReader{name: "test-reader"}
	opts := NewMetricsOptions(context.Background(), vertex, []HealthChecker{healthChecker}, []isb.LagReader{reader})
	assert.Equal(t, 3, len(opts))
	m := NewMetricsServer(vertex, opts...)
	assert.Equal(t, int64(120), m.lookbackSeconds)
	assert.Equal(t, 1, len(m.lagReaders))
	assert.Equal(t, reader, m.lagReaders["test-reader"])
}

type mockLagReader struct {
	name string
}

func (m *mockLagReader) GetName() string {
	return m.name
}

func (m *mockLagReader) Pending(ctx context.Context) (int64, error) {
	return 200, nil
}

type mockHealthChecker struct{}

func (m *mockHealthChecker) IsHealthy(ctx context.Context) error {
	return nil
}

func Test_MetricsServer_BuildAndExposePendingMetrics(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	vtx := &dfv1.Vertex{
		Spec: dfv1.VertexSpec{
			PipelineName: "test-pipeline",
			AbstractVertex: dfv1.AbstractVertex{
				Name: "test-vertex",
			},
		},
	}
	mockReader := &mockLagReader{name: "test-reader"}
	ms := NewMetricsServer(vtx, WithLagReaders(map[string]isb.LagReader{"test-reader": mockReader}), WithRefreshInterval(10*time.Millisecond))
	ms.lagCheckingInterval = 10 * time.Millisecond

	go ms.buildupPendingInfo(ctx)
	go ms.exposePendingMetrics(ctx)

	// Wait for a few ticks to expose metrics
	time.Sleep(50 * time.Millisecond)

	// Verify that metrics are exposed
	assert.NotEmpty(t, ms.partitionPendingInfo["test-reader"].Items())
	g, err := pending.GetMetricWithLabelValues("test-pipeline", "test-vertex", "1m", "test-reader")
	assert.NoError(t, err)
	m := &dto.Metric{}
	err = g.Write(m)
	assert.NoError(t, err)
	assert.Equal(t, float64(200), *m.GetGauge().Value)
}

func TestMetricsServer_CalculatePending(t *testing.T) {
	mockReader := &mockLagReader{name: "test-reader"}
	ms := NewMetricsServer(&dfv1.Vertex{}, WithLagReaders(map[string]isb.LagReader{"test-reader": mockReader}))

	// Test with no items
	pending := ms.calculatePending(60, "test-reader")
	assert.Equal(t, isb.PendingNotAvailable, pending)

	// Test with items within lookback window
	now := time.Now().Unix()
	ms.partitionPendingInfo["test-reader"].Append(timestampedPending{pending: 30, timestamp: now - 120})
	ms.partitionPendingInfo["test-reader"].Append(timestampedPending{pending: 10, timestamp: now - 30})
	ms.partitionPendingInfo["test-reader"].Append(timestampedPending{pending: 20, timestamp: now - 40})

	// Test with items in lookback window
	pending = ms.calculatePending(200, "test-reader")
	assert.Equal(t, int64(20), pending)

	pending = ms.calculatePending(60, "test-reader")
	assert.Equal(t, int64(15), pending)

	// Test with items within lookback window but no data points
	pending = ms.calculatePending(10, "test-reader")
	assert.Equal(t, isb.PendingNotAvailable, pending)
}
