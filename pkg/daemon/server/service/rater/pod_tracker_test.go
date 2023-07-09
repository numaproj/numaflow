package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type trackerMockHttpClient struct {
	podsCount int32
	lock      *sync.RWMutex
}

func (m *trackerMockHttpClient) setPodsCount(count int32) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.podsCount = count
}

func (m *trackerMockHttpClient) Get(url string) (*http.Response, error) {
	return nil, nil
}

func (m *trackerMockHttpClient) Head(url string) (*http.Response, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for i := 0; i < int(m.podsCount); i++ {
		if strings.Contains(url, "p-v-"+strconv.Itoa(i)+".p-v-headless.default.svc:2469/metrics") {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
		}
	}

	return nil, fmt.Errorf("pod not found")
}

func TestPodTracker_Start(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
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
	tracker := NewPodTracker(ctx, pipeline, WithRefreshInterval(time.Second))
	tracker.httpClient = &trackerMockHttpClient{
		podsCount: 10,
		lock:      &sync.RWMutex{},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := tracker.Start(ctx); err != nil {
			log.Fatalf("failed to start tracker: %v", err)
		}
	}()

	for tracker.activePods.Length() != 10 {
		select {
		case <-ctx.Done():
			t.Fatalf("incorrect active pods %v", ctx.Err())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	tracker.httpClient.(*trackerMockHttpClient).setPodsCount(5)

	for tracker.activePods.Length() != 5 {
		select {
		case <-ctx.Done():
			t.Fatalf("incorrect active pods %v", ctx.Err())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	cancel()
	wg.Wait()
}
