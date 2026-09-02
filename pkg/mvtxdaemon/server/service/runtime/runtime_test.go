package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type runtimeMockHTTPClient struct {
	lock         sync.RWMutex
	activePods   int
	responses    map[int]ErrorApiResponse
	getCallCount int
}

func (m *runtimeMockHTTPClient) Head(url string) (*http.Response, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for i := 0; i < m.activePods; i++ {
		if strings.Contains(url, fmt.Sprintf("mv-mv-%d.mv-mv-headless.default.svc:2470/runtime/errors", i)) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			}, nil
		}
	}
	return nil, fmt.Errorf("pod not found")
}

func (m *runtimeMockHTTPClient) Get(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.getCallCount++
	for i := 0; i < m.activePods; i++ {
		if !strings.Contains(url, fmt.Sprintf("mv-mv-%d.mv-mv-headless.default.svc:2470/runtime/errors", i)) {
			continue
		}
		response, ok := m.responses[i]
		if !ok {
			return nil, fmt.Errorf("no response configured for pod %d", i)
		}
		body, err := json.Marshal(response)
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	}
	return nil, fmt.Errorf("pod not found")
}

func newTestMonoVertex() *v1alpha1.MonoVertex {
	max := int32(1)
	return &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mv",
			Namespace: "default",
		},
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{
				Max: &max,
			},
		},
	}
}

func TestRuntimeCache_fetchesOnWorkerDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := &runtimeMockHTTPClient{
		responses: map[int]ErrorApiResponse{
			0: {
				Data: []ErrorDetails{{
					Container: "udf",
					Timestamp: 123,
					Code:      "E1",
					Message:   "failed",
				}},
			},
		},
	}
	r := NewRuntime(ctx, newTestMonoVertex(),
		WithRuntimeErrorsTimeStep(50*time.Millisecond),
		WithRuntimeHTTPClient(mockClient),
		WithPodTrackerOptions(
			WithRefreshInterval(time.Millisecond*50),
			WithInitialRefreshInterval(time.Millisecond*20),
			WithPodTrackerHTTPClient(mockClient),
		),
	).(*monoVertexRuntimeCache)

	require.NoError(t, r.StartCacheRefresher(ctx))

	mockClient.lock.Lock()
	mockClient.activePods = 1
	mockClient.lock.Unlock()
	r.podTracker.updateActivePods()

	assert.Eventually(t, func() bool {
		cache := r.GetLocalCache()
		errors, ok := cache["mv"]
		return ok && len(errors) == 1 && errors[0].ContainerErrors[0].Container == "udf"
	}, time.Second, time.Millisecond*10)
}

func TestRuntimeCache_retainsSnapshotAfterScaleToZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := &runtimeMockHTTPClient{
		activePods: 1,
		responses: map[int]ErrorApiResponse{
			0: {
				Data: []ErrorDetails{{
					Container: "udf",
					Timestamp: 123,
					Code:      "E1",
					Message:   "failed",
				}},
			},
		},
	}
	r := NewRuntime(ctx, newTestMonoVertex(),
		WithRuntimeErrorsTimeStep(time.Hour),
		WithRuntimeHTTPClient(mockClient),
		WithPodTrackerOptions(WithPodTrackerHTTPClient(mockClient)),
	).(*monoVertexRuntimeCache)

	require.NoError(t, r.StartCacheRefresher(ctx))

	assert.Eventually(t, func() bool {
		cache := r.GetLocalCache()
		errors, ok := cache["mv"]
		return ok && len(errors) == 1
	}, time.Second, time.Millisecond*10)

	mockClient.lock.Lock()
	mockClient.activePods = 0
	mockClient.lock.Unlock()
	r.podTracker.setActivePodsCount(0)

	time.Sleep(50 * time.Millisecond)

	cache := r.GetLocalCache()
	errors, ok := cache["mv"]
	require.True(t, ok)
	require.Len(t, errors, 1)
	assert.Equal(t, "udf", errors[0].ContainerErrors[0].Container)
}

func TestRuntimeCache_fetchFailureDoesNotClearSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockClient := &runtimeMockHTTPClient{
		activePods: 1,
		responses: map[int]ErrorApiResponse{
			0: {
				Data: []ErrorDetails{{
					Container: "udf",
					Timestamp: 123,
					Code:      "E1",
					Message:   "failed",
				}},
			},
		},
	}
	r := NewRuntime(ctx, newTestMonoVertex(),
		WithRuntimeErrorsTimeStep(time.Hour),
		WithRuntimeHTTPClient(mockClient),
		WithPodTrackerOptions(WithPodTrackerHTTPClient(mockClient)),
	).(*monoVertexRuntimeCache)

	require.NoError(t, r.StartCacheRefresher(ctx))

	assert.Eventually(t, func() bool {
		cache := r.GetLocalCache()
		errors, ok := cache["mv"]
		return ok && len(errors) == 1
	}, time.Second, time.Millisecond*10)

	mockClient.lock.Lock()
	delete(mockClient.responses, 0)
	mockClient.lock.Unlock()

	r.fetchAndPersistErrorForPod(0)

	cache := r.GetLocalCache()
	errors, ok := cache["mv"]
	require.True(t, ok)
	require.Len(t, errors, 1)
	assert.Equal(t, "udf", errors[0].ContainerErrors[0].Container)
}

func TestRuntimeCache_getLocalCacheReturnsCopy(t *testing.T) {
	ctx := context.Background()
	r := NewRuntime(ctx, newTestMonoVertex()).(*monoVertexRuntimeCache)

	r.cacheMutex.Lock()
	r.localCache["mv"] = []ReplicaErrors{{
		Replica: "mv-mv-0",
		ContainerErrors: []ErrorDetails{{
			Container: "udf",
		}},
	}}
	r.cacheMutex.Unlock()

	cacheCopy := r.GetLocalCache()
	cacheCopy["mv"][0].ContainerErrors[0].Container = "changed"

	cache := r.GetLocalCache()
	assert.Equal(t, "udf", cache["mv"][0].ContainerErrors[0].Container)
}
