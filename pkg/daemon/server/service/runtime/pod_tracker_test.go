package runtime

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/poddiscovery"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type mockHttpClient struct {
	podsCount    int32
	inactivePods map[int]bool
	lock         *sync.RWMutex
}

func (m *mockHttpClient) Head(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i := 0; i < int(m.podsCount); i++ {
		if m.inactivePods[i] {
			continue
		}
		if strings.Contains(url, "-"+strconv.Itoa(i)+".") &&
			strings.Contains(url, ".default.svc:2470/runtime/errors") {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
		}
	}

	return nil, fmt.Errorf("pod not found")
}
func (m *mockHttpClient) Get(url string) (*http.Response, error) {
	return nil, nil
}

type fakePodResolver struct {
	indicesByService map[string][]int
	errorsByService  map[string]error
}

func (f *fakePodResolver) Resolve(_ context.Context, req poddiscovery.ResolveRequest) ([]int, error) {
	return append([]int(nil), f.indicesByService[req.HeadlessService]...), f.errorsByService[req.HeadlessService]
}

func replicaIndices(count int) []int {
	indices := make([]int, count)
	for index := range indices {
		indices[index] = index
	}
	return indices
}

type recordingHTTPClient struct {
	lock *sync.Mutex
	urls []string
}

func (r *recordingHTTPClient) Get(url string) (*http.Response, error) {
	r.lock.Lock()
	r.urls = append(r.urls, url)
	r.lock.Unlock()
	return &http.Response{Body: io.NopCloser(strings.NewReader(`{"data":[]}`))}, nil
}

func (r *recordingHTTPClient) Head(string) (*http.Response, error) {
	return nil, nil
}

func TestNewPodTracker(t *testing.T) {
	ctx := context.Background()
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{
				{
					Name: "v",
				},
			},
		},
	}
	pt := NewPodTracker(ctx, pipeline)

	assert.NotNil(t, pt)
	assert.Equal(t, pipeline, pt.pipeline)
	assert.NotNil(t, pt.httpClient)
	assert.NotNil(t, pt.resolver)
	assert.Equal(t, 30*time.Second, pt.refreshInterval)
}

func TestPodTracker_Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{
				{
					Name: "v",
				},
			},
		},
	}
	pt := NewPodTracker(ctx, pipeline, WithPodResolver(&fakePodResolver{
		indicesByService: map[string][]int{"p-v-headless": replicaIndices(10)},
	}))
	pt.httpClient = &mockHttpClient{
		podsCount: 10,
		lock:      &sync.RWMutex{},
	}

	err := pt.Start(ctx)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Check if the active pods are being tracked
	assert.Equal(t, replicaIndices(10), pt.GetActivePodIndicesForVertex("v"))
}

func TestPodTracker_updateActivePodsToleratesInactiveGap(t *testing.T) {
	ctx := context.Background()
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec: v1alpha1.PipelineSpec{
			Vertices: []v1alpha1.AbstractVertex{{
				Name: "v",
			}},
		},
	}
	pt := NewPodTracker(ctx, pipeline, WithPodResolver(&fakePodResolver{
		indicesByService: map[string][]int{"p-v-headless": {0, 1, 2}},
	}))
	pt.httpClient = &mockHttpClient{
		podsCount:    3,
		inactivePods: map[int]bool{1: true},
		lock:         &sync.RWMutex{},
	}

	pt.updateActivePods(ctx)

	assert.Equal(t, []int{0, 2}, pt.GetActivePodIndicesForVertex("v"))
}

func TestPodTrackerResolverFailureIsIsolatedByVertex(t *testing.T) {
	ctx := context.Background()
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec:       v1alpha1.PipelineSpec{Vertices: []v1alpha1.AbstractVertex{{Name: "v"}, {Name: "other"}}},
	}
	resolver := &fakePodResolver{
		indicesByService: map[string][]int{
			"p-v-headless":     {0, 2},
			"p-other-headless": {0},
		},
		errorsByService: make(map[string]error),
	}
	pt := NewPodTracker(ctx, pipeline, WithPodResolver(resolver))
	pt.httpClient = &mockHttpClient{podsCount: 3, lock: &sync.RWMutex{}}
	pt.updateActivePods(ctx)
	resolver.indicesByService["p-v-headless"] = nil
	resolver.errorsByService["p-v-headless"] = fmt.Errorf("temporary DNS failure")
	resolver.indicesByService["p-other-headless"] = nil

	pt.updateActivePods(ctx)

	assert.Equal(t, []int{0, 2}, pt.GetActivePodIndicesForVertex("v"))
	assert.Empty(t, pt.GetActivePodIndicesForVertex("other"))
}

func TestRuntimeCacheFetchesExactActiveIndices(t *testing.T) {
	pipeline := &v1alpha1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec:       v1alpha1.PipelineSpec{Vertices: []v1alpha1.AbstractVertex{{Name: "v"}}},
	}
	tracker := NewPodTracker(context.Background(), pipeline)
	tracker.setActivePodIndices("v", []int{0, 2})
	client := &recordingHTTPClient{lock: &sync.Mutex{}}
	cache := &pipelineRuntimeCache{
		pipeline:   pipeline,
		localCache: make(map[string][]ReplicaErrors),
		podTracker: tracker,
		httpClient: client,
	}

	cache.fetchAndPersistErrors()

	assert.ElementsMatch(t, []string{
		"https://p-v-0.p-v-headless.default.svc:2470/runtime/errors",
		"https://p-v-2.p-v-headless.default.svc:2470/runtime/errors",
	}, client.urls)
}
