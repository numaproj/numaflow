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

package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

type mockHttpClient struct {
	podOneCount int64
	podTwoCount int64
}

func (m *mockHttpClient) Get(url string) (*http.Response, error) {
	if url == "https://p-v-0.p-v-headless.default.svc.cluster.local:2469/metrics" {
		m.podOneCount = m.podOneCount + 2000
		return &http.Response{
			StatusCode: 200,
			Body: io.NopCloser(bytes.NewReader([]byte(fmt.Sprintf(`
# HELP forwarder_read_total Total number of Messages Read
# TYPE forwarder_read_total counter
forwarder_read_total{buffer="input",pipeline="simple-pipeline",vertex="input"} %d
`, m.podOneCount))))}, nil
	} else if url == "https://p-v-1.p-v-headless.default.svc.cluster.local:2469/metrics" {
		m.podTwoCount = m.podTwoCount + 6000
		return &http.Response{
			StatusCode: 200,
			Body: io.NopCloser(bytes.NewReader([]byte(fmt.Sprintf(`
# HELP forwarder_read_total Total number of Messages Read
# TYPE forwarder_read_total counter
forwarder_read_total{buffer="input",pipeline="simple-pipeline",vertex="input"} %d
`, m.podTwoCount))))}, nil
	} else {
		return nil, nil
	}
}

func (m *mockHttpClient) Head(url string) (*http.Response, error) {
	if url == "https://p-v-0.p-v-headless.default.svc.cluster.local:2469/metrics" {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
	} else if url == "https://p-v-1.p-v-headless.default.svc.cluster.local:2469/metrics" {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
	} else {
		return nil, fmt.Errorf("unknown url: %s", url)
	}
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// TestRateCalculator_Start tests the rate calculator by mocking the http client
// we mock the metrics endpoint of the pods and increment the read count by 2000 for pod one, and 6000 for pod two,
// then we verify that the rate calculator is able to calculate the rate correctly
// since the refresh interval is the default 5 seconds, as we collect more and more metrics,
// the calculated rate should eventually be close (2000+6000)/5 = 1600
func TestRateCalculator_Start(t *testing.T) {
	rc := NewRateCalculator(&v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{
				{Name: "v"},
			},
		},
	}, &v1alpha1.AbstractVertex{
		Name: "v",
	})

	rc.httpClient = &mockHttpClient{podOneCount: 0, podTwoCount: 0}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()
	err := rc.Start(ctx)
	assert.NoError(t, err)
	ticker := time.NewTicker(rc.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timeout before getting the correct rate")
			return
		case <-ticker.C:
			// go test -race detects that here we have a race condition with rc.Start
			// the rate calculator can update the rates right after we check the rates
			// I think it's a false positive, since it's ok to read the rates while the rate calculator is updating the rates
			rates := rc.GetRates()
			if inDelta(rates["default"], 1600, 200) &&
				inDelta(rates["1m"], 1600, 200) &&
				inDelta(rates["5m"], 1600, 200) &&
				inDelta(rates["15m"], 1600, 200) {
				return
			}
		}
	}
}

func inDelta(value, expectedValue, delta float64) bool {
	return value > expectedValue-delta && value < expectedValue+delta
}
