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
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type mockHttpClient struct {
	podsCount int32
	lock      *sync.RWMutex
}

func (m *mockHttpClient) Head(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i := 0; i < int(m.podsCount); i++ {
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
	pt := NewPodTracker(ctx, mv)
	pt.httpClient = &mockHttpClient{
		podsCount: 10,
		lock:      &sync.RWMutex{},
	}

	err := pt.Start(ctx)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Check if the active pods are being tracked
	assert.Equal(t, pt.GetActivePodsCount(), 10)
}

func TestPodTracker_updateActivePods(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
	}
	pt := NewPodTracker(ctx, mv)
	pt.httpClient = &mockHttpClient{
		podsCount: 3,
		lock:      &sync.RWMutex{},
	}
	pt.updateActivePods()
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 3, pt.GetActivePodsCount())
}

func TestPodTracker_isActive(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
	}
	pt := NewPodTracker(ctx, mv)
	pt.httpClient = &mockHttpClient{
		podsCount: 3,
		lock:      &sync.RWMutex{},
	}

	time.Sleep(100 * time.Millisecond)
	active := pt.isActive("p-mv-0")
	assert.True(t, active)
	active = pt.isActive("p-mv-3")
	assert.False(t, active)
}

func TestPodTracker_setActivePodsCount(t *testing.T) {
	ctx := context.Background()
	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
	}

	pt := NewPodTracker(ctx, mv)
	pt.httpClient = &mockHttpClient{
		// active pods would be 3, from index 0..2
		podsCount: 3,
		lock:      &sync.RWMutex{},
	}
	// set active pods to 4
	pt.setActivePodsCount(4)
	assert.Equal(t, pt.GetActivePodsCount(), 4)
}
