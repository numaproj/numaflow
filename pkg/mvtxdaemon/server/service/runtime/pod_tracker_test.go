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
		if strings.Contains(url, "p-mv-"+strconv.Itoa(i)+".p-mv-headless.default.svc:2470/runtime/errors") {
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
	count int
	err   error
}

func (f *fakePodResolver) Resolve(context.Context, poddiscovery.ResolveRequest) (int, error) {
	return f.count, f.err
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
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
	}
	pt := NewPodTracker(ctx, mv)

	assert.NotNil(t, pt)
	assert.Equal(t, mv, pt.monoVertex)
	assert.NotNil(t, pt.httpClient)
	assert.NotNil(t, pt.resolver)
	assert.Equal(t, 30*time.Second, pt.refreshInterval)
}

func TestPodTracker_Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
	}
	pt := NewPodTracker(ctx, mv, WithPodResolver(&fakePodResolver{count: 10}))
	pt.httpClient = &mockHttpClient{
		podsCount: 10,
		lock:      &sync.RWMutex{},
	}

	err := pt.Start(ctx)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, 10, pt.GetActivePodsCount())
}

func TestPodTracker_updateActivePods(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
	}
	pt := NewPodTracker(ctx, mv, WithPodResolver(&fakePodResolver{count: 3}))
	pt.httpClient = &mockHttpClient{podsCount: 3, lock: &sync.RWMutex{}}
	pt.updateActivePods(ctx)
	assert.Equal(t, 3, pt.GetActivePodsCount())
}

func TestPodTracker_updateActivePodsToleratesInactiveGap(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
	}
	pt := NewPodTracker(ctx, mv, WithPodResolver(&fakePodResolver{count: 3}))
	pt.httpClient = &mockHttpClient{
		podsCount:    3,
		inactivePods: map[int]bool{1: true},
		lock:         &sync.RWMutex{},
	}

	pt.updateActivePods(ctx)

	assert.Equal(t, 3, pt.GetActivePodsCount())
}

func TestPodTrackerRetainsPreviousSnapshotOnResolverError(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
	}
	resolver := &fakePodResolver{count: 3}
	pt := NewPodTracker(ctx, mv, WithPodResolver(resolver))
	pt.httpClient = &mockHttpClient{podsCount: 3, lock: &sync.RWMutex{}}
	pt.updateActivePods(ctx)
	resolver.err = fmt.Errorf("temporary DNS failure")
	resolver.count = 0

	pt.updateActivePods(ctx)

	assert.Equal(t, 3, pt.GetActivePodsCount())
}

func TestPodTrackerEmptyDNSAnswerClearsSnapshot(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}
	resolver := &fakePodResolver{count: 1}
	pt := NewPodTracker(ctx, mv, WithPodResolver(resolver))
	pt.httpClient = &mockHttpClient{podsCount: 1, lock: &sync.RWMutex{}}
	pt.updateActivePods(ctx)
	resolver.count = 0

	pt.updateActivePods(ctx)

	assert.Equal(t, 0, pt.GetActivePodsCount())
}

func TestRuntimeCacheFetchesContiguousReplicas(t *testing.T) {
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
	}
	tracker := NewPodTracker(context.Background(), mv)
	tracker.setActivePodsCount(3)
	client := &recordingHTTPClient{lock: &sync.Mutex{}}
	cache := &monoVertexRuntimeCache{
		monoVtx:    mv,
		localCache: make(map[string][]ReplicaErrors),
		podTracker: tracker,
		httpClient: client,
	}

	cache.fetchAndPersistErrors()

	assert.ElementsMatch(t, []string{
		"https://p-mv-0.p-mv-headless.default.svc:2470/runtime/errors",
		"https://p-mv-1.p-mv-headless.default.svc:2470/runtime/errors",
		"https://p-mv-2.p-mv-headless.default.svc:2470/runtime/errors",
	}, client.urls)
}
