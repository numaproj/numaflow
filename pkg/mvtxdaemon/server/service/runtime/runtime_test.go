package runtime

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/stretchr/testify/assert"
)

type mockPodTracker struct {
	activePodIndexes []int
}

func (m *mockPodTracker) Start(ctx context.Context) error {
	return nil
}

func (m *mockPodTracker) GetActivePodIndexes() []int {
	return m.activePodIndexes
}

// mockHTTPClient implements monitorHttpClient interface for testing
type mockHTTPClient struct {
	getFunc  func(url string) (*http.Response, error)
	headFunc func(url string) (*http.Response, error)
}

func (m *mockHTTPClient) Get(url string) (*http.Response, error) {
	return m.getFunc(url)
}

func (m *mockHTTPClient) Head(url string) (*http.Response, error) {
	return m.headFunc(url)
}

// mockResponse creates a mock http.Response with given body
func mockResponse(body string) *http.Response {
	return &http.Response{
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

func TestNewRuntime(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{}

	mv.Name = "test-mono-vertex"
	runtime := NewRuntime(ctx, mv)

	assert.NotNil(t, runtime)
	assert.NotNil(t, runtime.localCache)
	assert.NotNil(t, runtime.httpClient)
	assert.NotNil(t, runtime.podTracker)
	assert.Equal(t, mv, runtime.monoVtx)
}

func TestRuntime_PersistRuntimeErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mv := &v1alpha1.MonoVertex{}
	mv.Name = "test-mono-vertex"

	testCases := []struct {
		name          string
		responseBody  string
		expectedCache map[PodReplica][]ErrorDetails
		shouldContain bool
	}{
		{
			name: "successful error persistence",
			responseBody: `{
				"error_message": "",
				"data": [
					{
						"container": "test-container",
						"timestamp": "2024-01-01T00:00:00Z",
						"code": "ERR001",
						"message": "Test error",
						"details": "Test details"
					}
				]
			}`,
			expectedCache: map[PodReplica][]ErrorDetails{
				"test-mono-vertex-mv-0": {
					{
						Container: "test-container",
						Timestamp: "2024-01-01T00:00:00Z",
						Code:      "ERR001",
						Message:   "Test error",
						Details:   "Test details",
					},
				},
			},
			shouldContain: true,
		},
		{
			name: "error message present",
			responseBody: `{
				"error_message": "some error",
				"data": []
			}`,
			expectedCache: map[PodReplica][]ErrorDetails{},
			shouldContain: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockClient := &mockHTTPClient{
				getFunc: func(url string) (*http.Response, error) {
					return mockResponse(tc.responseBody), nil
				},
			}

			runtime := NewRuntime(ctx, mv)
			runtime.httpClient = mockClient

			// Mock podTracker to return single active pod
			runtime.podTracker = &mockPodTracker{
				activePodIndexes: []int{0},
			}

			// Start PersistRuntimeErrors in a goroutine
			go runtime.PersistRuntimeErrors(ctx)

			// Wait for at least one tick
			time.Sleep(time.Second)

			// Check the cache
			runtime.cacheMutex.Lock()
			if tc.shouldContain {
				assert.Equal(t, tc.expectedCache["test-mono-vertex-mv-0"][0].Container, runtime.localCache[PodReplica("test-mono-vertex-mv-0")][0].Container)
				assert.Equal(t, tc.expectedCache["test-mono-vertex-mv-0"][0].Code, runtime.localCache[PodReplica("test-mono-vertex-mv-0")][0].Code)
			} else {
				assert.Len(t, runtime.localCache, 0)
			}
			runtime.cacheMutex.Unlock()
		})
	}
}

func TestRuntime_GetLocalCache(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{}

	runtime := NewRuntime(ctx, mv)

	// Add some test data to the cache
	testData := []ErrorDetails{
		{
			Container: "test-container",
			Timestamp: "2024-01-01T00:00:00Z",
			Code:      "ERR001",
			Message:   "Test error",
			Details:   "Test details",
		},
	}

	runtime.cacheMutex.Lock()
	runtime.localCache[PodReplica("test-mv-0")] = testData
	runtime.cacheMutex.Unlock()

	// Get the cache and verify
	cache := runtime.GetLocalCache()
	assert.Equal(t, testData, cache[PodReplica("test-mv-0")])
}

func TestRuntime_Start(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{}

	runtime := NewRuntime(ctx, mv)

	err := runtime.Start(ctx)
	assert.NoError(t, err)
}
