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
	"github.com/stretchr/testify/require"
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
	assert.Equal(t, 3, pt.GetActivePodsCount())
}

func TestPodTracker_zeroToActiveTransition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mv := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
	}
	mockClient := &mockHttpClient{
		podsCount: 0,
		lock:      &sync.RWMutex{},
	}
	pt := NewPodTracker(ctx, mv,
		WithRefreshInterval(time.Millisecond*50),
		WithInitialRefreshInterval(time.Millisecond*20),
		WithPodTrackerHTTPClient(mockClient),
	)

	err := pt.Start(ctx)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return pt.GetActivePodsCount() == 0
	}, time.Second, time.Millisecond*10)

	mockClient.lock.Lock()
	mockClient.podsCount = 1
	mockClient.lock.Unlock()

	assert.Eventually(t, func() bool {
		return pt.GetActivePodsCount() == 1
	}, time.Second, time.Millisecond*10)
}
