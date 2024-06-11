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
func TestMetricsServer_WithLagReaders(t *testing.T) {
	mockReader := &mockLagReader{name: "test-reader"}
	ms := NewMetricsServer(&dfv1.Vertex{}, WithLagReaders(map[string]isb.LagReader{
		"test-reader": mockReader,
	}))
	assert.Equal(t, 1, len(ms.lagReaders))
	assert.Equal(t, mockReader, ms.lagReaders["test-reader"])
}

func TestMetricsServer_WithRefreshInterval(t *testing.T) {
	interval := 10 * time.Second
	ms := NewMetricsServer(&dfv1.Vertex{}, WithRefreshInterval(interval))
	assert.Equal(t, interval, ms.refreshInterval)
}

func TestMetricsServer_WithLookbackSeconds(t *testing.T) {
	seconds := int64(300)
	ms := NewMetricsServer(&dfv1.Vertex{}, WithLookbackSeconds(seconds))
	assert.Equal(t, seconds, ms.lookbackSeconds)
}

func TestMetricsServer_WithHealthCheckExecutor(t *testing.T) {
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
	return 0, nil
}

type mockHealthChecker struct{}

func (m *mockHealthChecker) IsHealthy(ctx context.Context) error {
	return nil
}
