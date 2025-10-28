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

package rater

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

type raterMockHttpClient struct {
	podOneCount int64
	podTwoCount int64
	lock        *sync.RWMutex
}

func (m *raterMockHttpClient) Get(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if url == "https://p-v-0.p-v-headless.default.svc:2469/metrics" {
		m.podOneCount = m.podOneCount + 20
		resp := &http.Response{
			StatusCode: 200,
			// the test uses an abstract vertex without specifying vertex type, meaning it's neither source nor reduce,
			// hence the default forwarder metric name "forwarder_data_read" is used to retrieve the metric
			Body: io.NopCloser(bytes.NewReader([]byte(fmt.Sprintf(`
# HELP forwarder_data_read_total Total number of Messages Read
# TYPE forwarder_data_read_total counter
forwarder_data_read_total{buffer="input",pipeline="simple-pipeline",vertex="input",replica="0",partition_name="p-v-0"} %d
`, m.podOneCount))))}
		return resp, nil
	} else if url == "https://p-v-1.p-v-headless.default.svc:2469/metrics" {
		m.podTwoCount = m.podTwoCount + 60
		resp := &http.Response{
			StatusCode: 200,
			Body: io.NopCloser(bytes.NewReader([]byte(fmt.Sprintf(`
# HELP forwarder_data_read_total Total number of Messages Read
# TYPE forwarder_data_read_total counter
forwarder_data_read_total{buffer="input",pipeline="simple-pipeline",vertex="input",replica="0",partition_name="p-v-1"} %d
`, m.podTwoCount))))}
		return resp, nil
	} else {
		return nil, nil
	}
}

func (m *raterMockHttpClient) Head(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if url == "https://p-v-0.p-v-headless.default.svc:2469/metrics" {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
	} else if url == "https://p-v-1.p-v-headless.default.svc:2469/metrics" {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
	} else {
		return nil, fmt.Errorf("unknown url: %s", url)
	}
}

// TestRater_Start tests the rater by mocking the http client
// we mock the metrics endpoint of the pods and increment the read count by 20 for pod one, and 60 for pod two,
// then we verify that the rate calculator is able to calculate a positive rate for the vertex
// note: this test doesn't test the accuracy of the calculated rate, the calculation is tested by helper_test.go
func TestRater_Start(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*29)
	lookBackSeconds := uint32(30)
	defer cancel()
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{
				{
					Name:  "v",
					Scale: v1alpha1.Scale{LookbackSeconds: &lookBackSeconds},
				},
			},
		},
	}
	r := NewRater(ctx, pipeline, WithTaskInterval(time.Second))
	podTracker := NewPodTracker(ctx, pipeline, WithRefreshInterval(time.Second*1))
	podTracker.httpClient = &raterMockHttpClient{podOneCount: 0, podTwoCount: 0, lock: &sync.RWMutex{}}
	r.httpClient = &raterMockHttpClient{podOneCount: 0, podTwoCount: 0, lock: &sync.RWMutex{}}
	r.podTracker = podTracker

	timer := time.NewTimer(60 * time.Second)
	succeedChan := make(chan struct{})
	go func() {
		if err := r.Start(ctx); err != nil {
			log.Fatalf("failed to start rater: %v", err)
		}
	}()
	go func() {
		for {
			if r.GetRates("v", "p-v-0")["default"].GetValue() <= 0 || r.GetRates("v", "p-v-1")["default"].GetValue() <= 0 {
				time.Sleep(time.Second)
			} else {
				succeedChan <- struct{}{}
				break
			}
		}
	}()
	select {
	case <-succeedChan:
		time.Sleep(time.Second)
		break
	case <-timer.C:
		t.Fatalf("timed out waiting for rate to be calculated")
	}
	timer.Stop()
}

// TestRater_updateDynamicLookbackSecs tests the dynamic lookback update functionality
func TestRater_updateDynamicLookbackSecs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name                   string
		setupTimestampedCounts func() map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]
		expectedUpdated        bool
		expectedValue          float64
		vertexLookbackSecs     uint32
	}{
		{
			name: "no update when counts length <= 1",
			setupTimestampedCounts: func() map[string]*sharedqueue.OverflowQueue[*TimestampedCounts] {
				q := sharedqueue.New[*TimestampedCounts](180)
				tc := NewTimestampedCounts(time.Now().Unix())
				tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
				q.Append(tc)
				return map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]{"v1": q}
			},
			expectedUpdated:    false,
			expectedValue:      60.0, // original value
			vertexLookbackSecs: 60,
		},
		{
			name: "no update when startIndex not found",
			setupTimestampedCounts: func() map[string]*sharedqueue.OverflowQueue[*TimestampedCounts] {
				q := sharedqueue.New[*TimestampedCounts](180)
				now := time.Now().Unix()
				// Add counts that are too old (older than 3 * lookback seconds)
				tc1 := NewTimestampedCounts(now - 300) // 5 minutes ago
				tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
				tc2 := NewTimestampedCounts(now - 290)
				tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
				q.Append(tc1)
				q.Append(tc2)
				return map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]{"v1": q}
			},
			expectedUpdated:    false,
			expectedValue:      60.0,
			vertexLookbackSecs: 60,
		},
		{
			name: "no update when time diff is 0",
			setupTimestampedCounts: func() map[string]*sharedqueue.OverflowQueue[*TimestampedCounts] {
				q := sharedqueue.New[*TimestampedCounts](180)
				now := time.Now().Unix()
				// Add counts with same timestamp
				tc1 := NewTimestampedCounts(now - 60)
				tc1.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
				tc2 := NewTimestampedCounts(now - 60) // same timestamp
				tc2.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 200.0}})
				tc3 := NewTimestampedCounts(now - 50)
				tc3.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 300.0}})
				q.Append(tc1)
				q.Append(tc2)
				q.Append(tc3)
				return map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]{"v1": q}
			},
			expectedUpdated:    false,
			expectedValue:      60.0,
			vertexLookbackSecs: 60,
		},
		{
			name: "update when calculated lookback is higher",
			setupTimestampedCounts: func() map[string]*sharedqueue.OverflowQueue[*TimestampedCounts] {
				q := sharedqueue.New[*TimestampedCounts](180)
				now := time.Now().Unix()

				// Create a scenario where data processing is slow, requiring higher lookback
				timestamps := []int64{now - 120, now - 110, now - 100, now - 90, now - 80, now - 70, now - 60, now - 50, now - 40, now - 30}
				count := 100.0
				for _, ts := range timestamps {
					tc := NewTimestampedCounts(ts)
					tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": count}})
					q.Append(tc)
					// Simulate very slow processing - count stays same for long periods
					if ts > now-70 {
						count += 1 // slow increase
					}
				}
				return map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]{"v1": q}
			},
			expectedUpdated:    true,
			expectedValue:      120.0, // rounded up to next minute
			vertexLookbackSecs: 60,
		},
		{
			name: "test MaxLookbackSeconds limit (3*currentLookback)",
			setupTimestampedCounts: func() map[string]*sharedqueue.OverflowQueue[*TimestampedCounts] {
				q := sharedqueue.New[*TimestampedCounts](180)
				now := time.Now().Unix()

				// findStartIndex uses 3*currentLookback (180s) to find startIndex
				// So we need to create data within the last 180 seconds that would calculate > 600s lookback
				// But since we're limited to 180s window, we can't actually hit 600s limit in this test
				// This test demonstrates the current behavior, not the 600s limit
				timestamps := []int64{
					now - 180, now - 170, now - 160, now - 150, now - 140, now - 130, now - 120, now - 110, now - 100, now - 90,
					now - 80, now - 70, now - 60, now - 50, now - 40, now - 30, now - 20, now - 10,
				}

				count := 100.0
				for i, ts := range timestamps {
					tc := NewTimestampedCounts(ts)
					tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": count}})
					q.Append(tc)
					// Change only happens at the very end, creating a long unchanged period within 180s window
					if i == len(timestamps)-1 {
						count = 200.0
					}
				}
				return map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]{"v1": q}
			},
			expectedUpdated:    true,
			expectedValue:      180.0, // This is the actual behavior - limited by 3*currentLookback window
			vertexLookbackSecs: 60,
		},
		{
			name: "test MaxLookbackSeconds limit with high current lookback (600s)",
			setupTimestampedCounts: func() map[string]*sharedqueue.OverflowQueue[*TimestampedCounts] {
				q := sharedqueue.New[*TimestampedCounts](180)
				now := time.Now().Unix()

				// Create data that spans a longer period (3*400 = 1200s window)
				// This simulates a scenario where current lookback is 400s, so 3*400 = 1200s window
				timestamps := make([]int64, 120) // 120 data points over 20 minutes
				for i := 0; i < 120; i++ {
					timestamps[i] = now - int64(1200-i*10) // spread over 20 minutes
				}

				count := 100.0
				for i, ts := range timestamps {
					tc := NewTimestampedCounts(ts)
					tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": count}})
					q.Append(tc)
					// Only change count at the very end to simulate 20 minutes of no processing
					if i == len(timestamps)-1 {
						count = 300.0 // big jump after 20 minutes of no change
					}
				}
				return map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]{"v1": q}
			},
			expectedUpdated:    true,
			expectedValue:      600.0, // should be capped at MaxLookbackSeconds
			vertexLookbackSecs: 400,   // Start with high lookback to allow 3*400 = 1200s window
		},
		{
			name: "update respects minimum lookback from vertex scale",
			setupTimestampedCounts: func() map[string]*sharedqueue.OverflowQueue[*TimestampedCounts] {
				q := sharedqueue.New[*TimestampedCounts](180)
				now := time.Now().Unix()

				// Create a scenario with very fast processing
				timestamps := []int64{now - 120, now - 110, now - 100, now - 90, now - 80, now - 70, now - 60, now - 50}
				count := 100.0
				for _, ts := range timestamps {
					tc := NewTimestampedCounts(ts)
					tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": count}})
					q.Append(tc)
					count += 100 // fast processing
				}
				return map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]{"v1": q}
			},
			expectedUpdated:    false, // should stay at minimum
			expectedValue:      120.0, // vertex minimum
			vertexLookbackSecs: 120,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create pipeline with vertex
			pipeline := &v1alpha1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pipeline",
					Namespace: "default",
				},
				Spec: v1alpha1.PipelineSpec{
					Vertices: []v1alpha1.AbstractVertex{
						{
							Name:  "v1",
							Scale: v1alpha1.Scale{LookbackSeconds: &tt.vertexLookbackSecs},
						},
					},
				},
			}

			// Create rater
			rater := NewRater(ctx, pipeline)

			// Set up timestamped counts
			rater.timestampedPodCounts = tt.setupTimestampedCounts()

			// Store original value
			originalValue := rater.lookBackSeconds["v1"].Load()

			// Call the function under test
			rater.updateDynamicLookbackSecs()

			// Check results
			newValue := rater.lookBackSeconds["v1"].Load()
			if tt.expectedUpdated {
				assert.NotEqual(t, originalValue, newValue, "Expected lookback to be updated")
				assert.Equal(t, tt.expectedValue, newValue, "Expected specific updated value")
			} else {
				assert.Equal(t, originalValue, newValue, "Expected lookback to remain unchanged")
			}
		})
	}
}

// TestFindStartIndex tests the findStartIndex helper function
func TestFindStartIndex(t *testing.T) {
	now := time.Now().Unix()

	tests := []struct {
		name            string
		lookbackSeconds int64
		counts          []*TimestampedCounts
		expectedIndex   int
	}{
		{
			name:            "empty counts",
			lookbackSeconds: 60,
			counts:          []*TimestampedCounts{},
			expectedIndex:   indexNotFound,
		},
		{
			name:            "single count",
			lookbackSeconds: 60,
			counts: []*TimestampedCounts{
				NewTimestampedCounts(now - 30),
			},
			expectedIndex: indexNotFound,
		},
		{
			name:            "second last element outside lookback",
			lookbackSeconds: 60,
			counts: []*TimestampedCounts{
				NewTimestampedCounts(now - 120),
				NewTimestampedCounts(now - 100),
			},
			expectedIndex: indexNotFound,
		},
		{
			name:            "find correct start index",
			lookbackSeconds: 60,
			counts: []*TimestampedCounts{
				NewTimestampedCounts(now - 90), // outside window
				NewTimestampedCounts(now - 70), // outside window
				NewTimestampedCounts(now - 50), // inside window - should be start
				NewTimestampedCounts(now - 30), // inside window
				NewTimestampedCounts(now - 10), // inside window (but ignored as it's last element)
			},
			expectedIndex: 2,
		},
		{
			name:            "all elements within lookback",
			lookbackSeconds: 120,
			counts: []*TimestampedCounts{
				NewTimestampedCounts(now - 90),
				NewTimestampedCounts(now - 70),
				NewTimestampedCounts(now - 50),
				NewTimestampedCounts(now - 30),
				NewTimestampedCounts(now - 10),
			},
			expectedIndex: 0,
		},
		{
			name:            "exact boundary condition",
			lookbackSeconds: 60,
			counts: []*TimestampedCounts{
				NewTimestampedCounts(now - 90),
				NewTimestampedCounts(now - 60), // exactly at boundary
				NewTimestampedCounts(now - 30),
				NewTimestampedCounts(now - 10),
			},
			expectedIndex: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := findStartIndex(tt.lookbackSeconds, tt.counts)
			assert.Equal(t, tt.expectedIndex, result)
		})
	}
}

// TestCalculateLookback tests the CalculateLookback helper function
func TestCalculateLookback(t *testing.T) {
	tests := []struct {
		name             string
		counts           []*TimestampedCounts
		startIndex       int
		endIndex         int
		expectedLookback int64
	}{
		{
			name: "no data changes - returns 0",
			counts: func() []*TimestampedCounts {
				now := time.Now().Unix()
				tc := NewTimestampedCounts(now)
				tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": 100.0}})
				return []*TimestampedCounts{tc}
			}(),
			startIndex:       0,
			endIndex:         0,
			expectedLookback: 0,
		},
		{
			name: "single partition with constant processing",
			counts: func() []*TimestampedCounts {
				now := time.Now().Unix()
				counts := []*TimestampedCounts{
					NewTimestampedCounts(now - 60),
					NewTimestampedCounts(now - 50),
					NewTimestampedCounts(now - 40),
					NewTimestampedCounts(now - 30),
				}
				// Simulate constant count (no processing) for 30 seconds
				count := 100.0
				for i, tc := range counts {
					if i < 3 { // first 3 have same count
						tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": count}})
					} else { // last one has increased count
						tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": count + 50}})
					}
				}
				return counts
			}(),
			startIndex:       0,
			endIndex:         3,
			expectedLookback: 30, // 30 seconds of no change
		},
		{
			name: "multiple partitions with different processing speeds",
			counts: func() []*TimestampedCounts {
				now := time.Now().Unix()
				counts := []*TimestampedCounts{
					NewTimestampedCounts(now - 90),
					NewTimestampedCounts(now - 80),
					NewTimestampedCounts(now - 70),
					NewTimestampedCounts(now - 60),
					NewTimestampedCounts(now - 50),
				}

				// Partition1: slower processing (30 seconds no change: from index 0 to 2, changes at index 3)
				// Partition2: faster processing (20 seconds no change: from index 0 to 1, changes at index 2)
				for i, tc := range counts {
					p1Count := 100.0
					p2Count := 200.0

					// Partition1 changes at index 3 (30 seconds: 90->60)
					if i >= 3 {
						p1Count = 150.0
					}
					// Partition2 changes at index 2 (20 seconds: 90->70)
					if i >= 2 {
						p2Count = 250.0
					}

					tc.Update(&PodReadCount{"pod1", map[string]float64{
						"partition1": p1Count,
						"partition2": p2Count,
					}})
				}
				return counts
			}(),
			startIndex:       0,
			endIndex:         4,
			expectedLookback: 30, // maximum of the unchanged durations
		},
		{
			name: "processing with multiple pods",
			counts: func() []*TimestampedCounts {
				now := time.Now().Unix()
				counts := []*TimestampedCounts{
					NewTimestampedCounts(now - 50),
					NewTimestampedCounts(now - 40),
					NewTimestampedCounts(now - 30),
					NewTimestampedCounts(now - 20),
				}

				// Multiple pods processing same partition
				for i, tc := range counts {
					p1Count1 := 100.0
					p1Count2 := 200.0

					// Both pods process after 20 seconds
					if i >= 2 {
						p1Count1 = 150.0
						p1Count2 = 250.0
					}

					tc.Update(&PodReadCount{"pod1", map[string]float64{"partition1": p1Count1}})
					tc.Update(&PodReadCount{"pod2", map[string]float64{"partition1": p1Count2}})
				}
				return counts
			}(),
			startIndex:       0,
			endIndex:         3,
			expectedLookback: 20, // 20 seconds until processing resumed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateLookback(tt.counts, tt.startIndex, tt.endIndex)
			assert.Equal(t, tt.expectedLookback, result)
		})
	}
}
