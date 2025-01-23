package rater

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// Mock HTTP client to simulate API responses
type processingTimeMockHttpClient struct {
	podOneCount int64
	podTwoCount int64
	lock        *sync.RWMutex
}

func (m *processingTimeMockHttpClient) Get(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if url == "https://p-mv-0.p-mv-headless.default.svc:2469/metrics" {
		m.podOneCount += 20
		resp := &http.Response{
			StatusCode: 200,
			Body: io.NopCloser(bytes.NewReader([]byte(fmt.Sprintf(`

# HELP monovtx_processing_time A Histogram to keep track of the total time taken to forward a chunk, in microseconds.
# TYPE monovtx_processing_time histogram
monovtx_processing_time_sum{mvtx_name="simple-mono-vertex",mvtx_replica="0"} %d
monovtx_processing_time_count{mvtx_name="simple-mono-vertex",mvtx_replica="0"} 1
`, m.podOneCount))))}
		return resp, nil
	} else if url == "https://p-mv-1.p-mv-headless.default.svc:2469/metrics" {
		m.podTwoCount += 60
		resp := &http.Response{
			StatusCode: 200,
			Body: io.NopCloser(bytes.NewReader([]byte(fmt.Sprintf(`
# HELP monovtx_processing_time A Histogram to keep track of the total time taken to forward a chunk, in microseconds.
# TYPE monovtx_processing_time histogram
monovtx_processing_time_sum{mvtx_name="simple-mono-vertex",mvtx_replica="0"} %d
monovtx_processing_time_count{mvtx_name="simple-mono-vertex",mvtx_replica="0"} 1
`, m.podTwoCount))))}
		return resp, nil
	} else {
		return nil, fmt.Errorf("unknown url: %s", url)
	}
}

func (m *processingTimeMockHttpClient) Head(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if url == "https://p-mv-0.p-mv-headless.default.svc:2469/metrics" {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
	} else if url == "https://p-mv-1.p-mv-headless.default.svc:2469/metrics" {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
	} else {
		return nil, fmt.Errorf("unknown url: %s", url)
	}
}

// Unit Tests for PodProcessingTime
func TestPodProcessingTime_Name(t *testing.T) {
	podProcessingTime := &PodProcessingTime{name: "test-pod"}
	assert.Equal(t, "test-pod", podProcessingTime.Name())
}

func TestPodProcessingTime_processingTimeValues(t *testing.T) {
	podProcessingTime := &PodProcessingTime{processingTimeSum: 100.0, processingTimeCount: 10.0}
	sum, count := podProcessingTime.processingTimeValues()
	assert.Equal(t, 100.0, sum)
	assert.Equal(t, 10.0, count)
}

func TestRater_updateDynamicLookbackSecs_stepUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*29)
	defer cancel()
	lookBackSeconds := uint32(120)
	mvtx := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{LookbackSeconds: &lookBackSeconds},
		},
	}
	rater := NewRater(ctx, mvtx, WithTaskInterval(1000))

	rater.httpClient = &processingTimeMockHttpClient{podOneCount: 0, podTwoCount: 10, lock: &sync.RWMutex{}}

	// Test: No update with insufficient data
	oldLookback := rater.userSpecifiedLookBackSeconds.Load()
	rater.updateDynamicLookbackSecs()
	assert.Equal(t, oldLookback, rater.userSpecifiedLookBackSeconds.Load())

	startTime := time.Now()
	tc1 := NewTimestampedProcessingTime(startTime.Truncate(CountWindow).Unix() - 20)
	tc1.Update(&PodProcessingTime{"pod1", 400000000.0, 1})
	rater.timestampedPodProcessingTime.Append(tc1)

	tc2 := NewTimestampedProcessingTime(startTime.Truncate(CountWindow).Unix() - 10)
	tc2.Update(&PodProcessingTime{"pod1", 400000000.0, 1})
	rater.timestampedPodProcessingTime.Append(tc2)

	tc3 := NewTimestampedProcessingTime(startTime.Truncate(CountWindow).Unix())
	tc3.Update(&PodProcessingTime{"pod1", 400000000.0, 1})
	rater.timestampedPodProcessingTime.Append(tc3)

	// Test: Update case when processing time exceeds current lookback
	rater.updateDynamicLookbackSecs()
	newLookback := rater.userSpecifiedLookBackSeconds.Load()
	assert.NotEqual(t, oldLookback, newLookback)
	assert.Greater(t, newLookback, oldLookback)
	assert.Equal(t, newLookback, float64(420))
}

func TestRater_updateDynamicLookbackSecs_max(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*29)
	defer cancel()
	lookBackSeconds := uint32(120)
	mvtx := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{LookbackSeconds: &lookBackSeconds},
		},
	}
	rater := NewRater(ctx, mvtx, WithTaskInterval(1000))

	rater.httpClient = &processingTimeMockHttpClient{podOneCount: 0, podTwoCount: 10, lock: &sync.RWMutex{}}

	// Test: No update with insufficient data
	oldLookback := rater.userSpecifiedLookBackSeconds.Load()
	rater.updateDynamicLookbackSecs()
	assert.Equal(t, oldLookback, rater.userSpecifiedLookBackSeconds.Load())

	startTime := time.Now()
	tc1 := NewTimestampedProcessingTime(startTime.Truncate(CountWindow).Unix() - 20)
	tc1.Update(&PodProcessingTime{"pod1", 4000000000.0, 1})
	rater.timestampedPodProcessingTime.Append(tc1)

	tc2 := NewTimestampedProcessingTime(startTime.Truncate(CountWindow).Unix() - 10)
	tc2.Update(&PodProcessingTime{"pod1", 4000000000.0, 1})
	rater.timestampedPodProcessingTime.Append(tc2)

	tc3 := NewTimestampedProcessingTime(startTime.Truncate(CountWindow).Unix())
	tc3.Update(&PodProcessingTime{"pod1", 4000000000.0, 1})
	rater.timestampedPodProcessingTime.Append(tc3)

	// Test: Update case when processing time exceeds max lookback
	rater.updateDynamicLookbackSecs()
	newLookback := rater.userSpecifiedLookBackSeconds.Load()
	assert.NotEqual(t, oldLookback, newLookback)
	assert.Greater(t, newLookback, oldLookback)
	assert.Equal(t, newLookback, float64(600))
}

func TestRater_updateDynamicLookbackSecs_min(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*29)
	defer cancel()
	lookBackSeconds := uint32(120)
	mvtx := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{LookbackSeconds: &lookBackSeconds},
		},
	}
	rater := NewRater(ctx, mvtx, WithTaskInterval(1000))

	rater.httpClient = &processingTimeMockHttpClient{podOneCount: 0, podTwoCount: 10, lock: &sync.RWMutex{}}

	// Test: No update with insufficient data
	oldLookback := rater.userSpecifiedLookBackSeconds.Load()
	rater.updateDynamicLookbackSecs()
	assert.Equal(t, oldLookback, rater.userSpecifiedLookBackSeconds.Load())

	startTime := time.Now()
	tc1 := NewTimestampedProcessingTime(startTime.Truncate(CountWindow).Unix() - 20)
	tc1.Update(&PodProcessingTime{"pod1", 40000.0, 1})
	rater.timestampedPodProcessingTime.Append(tc1)

	tc2 := NewTimestampedProcessingTime(startTime.Truncate(CountWindow).Unix() - 10)
	tc2.Update(&PodProcessingTime{"pod1", 40000.0, 1})
	rater.timestampedPodProcessingTime.Append(tc2)

	tc3 := NewTimestampedProcessingTime(startTime.Truncate(CountWindow).Unix())
	tc3.Update(&PodProcessingTime{"pod1", 40000.0, 1})
	rater.timestampedPodProcessingTime.Append(tc3)

	// Test: Update case when processing time exceeds max lookback
	rater.updateDynamicLookbackSecs()
	newLookback := rater.userSpecifiedLookBackSeconds.Load()
	assert.Equal(t, oldLookback, newLookback)
	assert.Equal(t, newLookback, float64(120))
}

func TestRater_updateDynamicLookbackSecs_stepDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*29)
	defer cancel()
	lookBackSeconds := uint32(120)
	mvtx := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{LookbackSeconds: &lookBackSeconds},
		},
	}
	rater := NewRater(ctx, mvtx, WithTaskInterval(1000))

	rater.httpClient = &processingTimeMockHttpClient{podOneCount: 0, podTwoCount: 10, lock: &sync.RWMutex{}}

	// Test: No update with insufficient data
	oldLookback := rater.userSpecifiedLookBackSeconds.Load()
	rater.updateDynamicLookbackSecs()
	assert.Equal(t, oldLookback, rater.userSpecifiedLookBackSeconds.Load())

	startTime := time.Now()
	i := startTime.Truncate(CountWindow).Unix()

	//// Test: Step up case
	// Prepare a mock timeline of processing times
	timeline := []*TimestampedProcessingTime{
		{timestamp: i, podProcessingTime: map[string]float64{"pod1": 400}, lock: &sync.RWMutex{}},
		{timestamp: i + 60, podProcessingTime: map[string]float64{"pod1": 400}, lock: &sync.RWMutex{}},
		{timestamp: i + 120, podProcessingTime: map[string]float64{"pod1": 400}, lock: &sync.RWMutex{}},
		{timestamp: i + 180, podProcessingTime: map[string]float64{"pod1": 400}, lock: &sync.RWMutex{}},
		{timestamp: i + 240, podProcessingTime: map[string]float64{"pod1": 400}, lock: &sync.RWMutex{}},
	}

	for _, v := range timeline {
		rater.timestampedPodProcessingTime.Append(v)
	}

	// Test: Update case when processing time exceeds current lookback
	rater.updateDynamicLookbackSecs()
	newLookback := rater.userSpecifiedLookBackSeconds.Load()
	assert.NotEqual(t, oldLookback, newLookback)
	assert.Greater(t, newLookback, oldLookback)
	assert.Equal(t, newLookback, float64(420))

	//// Test: Step down case
	// Prepare a mock timeline of processing times
	newTimeline := []*TimestampedProcessingTime{
		{timestamp: i + 300, podProcessingTime: map[string]float64{"pod1": 200}, lock: &sync.RWMutex{}},
		{timestamp: i + 400, podProcessingTime: map[string]float64{"pod1": 200}, lock: &sync.RWMutex{}},
		{timestamp: i + 500, podProcessingTime: map[string]float64{"pod1": 200}, lock: &sync.RWMutex{}},
		{timestamp: i + 600, podProcessingTime: map[string]float64{"pod1": 200}, lock: &sync.RWMutex{}},
	}

	for _, v := range newTimeline {
		rater.timestampedPodProcessingTime.Append(v)
	}

	rater.updateDynamicLookbackSecs()
	stepDownLookback := rater.userSpecifiedLookBackSeconds.Load()
	assert.NotEqual(t, newLookback, stepDownLookback)
	assert.Less(t, stepDownLookback, newLookback)
	assert.Equal(t, stepDownLookback, float64(360))
}

func TestRater_getPodProcessingTime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*29)
	defer cancel()
	lookBackSeconds := uint32(30)
	mvtx := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{LookbackSeconds: &lookBackSeconds},
		},
	}
	rater := NewRater(ctx, mvtx, WithTaskInterval(1000))

	rater.httpClient = &processingTimeMockHttpClient{podOneCount: 0, podTwoCount: 10, lock: &sync.RWMutex{}}

	// Case: Valid response for pod 0
	podProcessingTime := rater.getPodProcessingTime("p-mv-0")
	require.NotNil(t, podProcessingTime)
	assert.Equal(t, 20.0, podProcessingTime.processingTimeSum)
	assert.Equal(t, float64(1), podProcessingTime.processingTimeCount)

	// Case: Valid response for pod 1
	podProcessingTime = rater.getPodProcessingTime("p-mv-1")
	require.NotNil(t, podProcessingTime)
	assert.Equal(t, 70.0, podProcessingTime.processingTimeSum)
	assert.Equal(t, float64(1), podProcessingTime.processingTimeCount)

	// Case: Unknown URL
	podProcessingTime = rater.getPodProcessingTime("unknown-pod")
	assert.Nil(t, podProcessingTime)
}
