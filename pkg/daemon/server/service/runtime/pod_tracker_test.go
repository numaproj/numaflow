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
		if strings.Contains(url, "p-v-"+strconv.Itoa(i)+".p-v-headless.default.svc:2470/runtime/errors") {
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
	pt := NewPodTracker(ctx, pipeline)
	pt.httpClient = &mockHttpClient{
		podsCount: 10,
		lock:      &sync.RWMutex{},
	}

	err := pt.Start(ctx)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Check if the active pods are being tracked
	assert.Equal(t, pt.GetActivePodsCountForVertex("v"), 10)
}
