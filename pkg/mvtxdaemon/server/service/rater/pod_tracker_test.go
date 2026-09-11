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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/numaproj/numaflow/pkg/shared/poddiscovery"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

type trackerMockHttpClient struct {
	podsCount    int32
	inactivePods map[int]bool
	lock         *sync.RWMutex
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
		if m.inactivePods[i] {
			continue
		}
		if strings.Contains(url, "p-mv-"+strconv.Itoa(i)+".p-mv-headless.default.svc:2469/metrics") {
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(bytes.NewReader([]byte(``)))}, nil
		}
	}

	return nil, fmt.Errorf("pod not found")
}

type fakePodResolver struct {
	indices []int
	err     error
}

func (f *fakePodResolver) Resolve(context.Context, poddiscovery.ResolveRequest) ([]int, error) {
	return append([]int(nil), f.indices...), f.err
}

func replicaIndices(count int) []int {
	indices := make([]int, count)
	for index := range indices {
		indices[index] = index
	}
	return indices
}

func TestPodTracker_Start(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	lookBackSeconds := uint32(30)
	defer cancel()
	pipeline := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "p",
			Namespace: "default",
		},
		Spec: v1alpha1.MonoVertexSpec{
			Scale: v1alpha1.Scale{LookbackSeconds: &lookBackSeconds},
		},
	}
	tracker := NewPodTracker(
		ctx,
		pipeline,
		WithRefreshInterval(time.Second),
		WithPodResolver(&fakePodResolver{indices: replicaIndices(10)}),
	)
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

	for tracker.GetActivePodsCount() != 10 {
		select {
		case <-ctx.Done():
			t.Fatalf("incorrect active pods %v", ctx.Err())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	tracker.httpClient.(*trackerMockHttpClient).setPodsCount(5)

	for tracker.GetActivePodsCount() != 5 {
		select {
		case <-ctx.Done():
			t.Fatalf("incorrect active pods %v", ctx.Err())
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	cancel()
	wg.Wait()

	assert.NotEmpty(t, tracker.LeastRecentlyUsed())
	assert.Equal(t, true, tracker.IsActive("p*4"))
	assert.Equal(t, false, tracker.IsActive("p*5"))
}

func TestPodTracker_updateActivePodsToleratesInactiveGap(t *testing.T) {
	ctx := context.Background()
	monoVertex := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
	}
	tracker := NewPodTracker(ctx, monoVertex, WithPodResolver(&fakePodResolver{indices: []int{0, 1, 2}}))
	tracker.httpClient = &trackerMockHttpClient{
		podsCount:    3,
		inactivePods: map[int]bool{1: true},
		lock:         &sync.RWMutex{},
	}

	tracker.updateActivePods(ctx)

	assert.True(t, tracker.IsActive("p*0"))
	assert.False(t, tracker.IsActive("p*1"))
	assert.True(t, tracker.IsActive("p*2"))
}

func TestPodTrackerResolverErrorRetainsAndEmptyAnswerClears(t *testing.T) {
	ctx := context.Background()
	monoVertex := &v1alpha1.MonoVertex{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
	}
	resolver := &fakePodResolver{indices: []int{0, 2}}
	tracker := NewPodTracker(ctx, monoVertex, WithPodResolver(resolver))
	tracker.httpClient = &trackerMockHttpClient{podsCount: 3, lock: &sync.RWMutex{}}
	tracker.updateActivePods(ctx)

	resolver.indices = nil
	resolver.err = fmt.Errorf("temporary DNS failure")
	tracker.updateActivePods(ctx)
	assert.True(t, tracker.IsActive("p*0"))
	assert.True(t, tracker.IsActive("p*2"))

	resolver.err = nil
	tracker.updateActivePods(ctx)
	assert.Equal(t, 0, tracker.GetActivePodsCount())
}

func TestPodTracker_GetPodInfo(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	tracker := NewPodTracker(ctx, nil, WithRefreshInterval(time.Second))
	// error scenario - more than 3 fields
	podInfo, err := tracker.GetPodInfo("p*v*0*1")
	assert.Nilf(t, podInfo, "podInfo should be nil")
	assert.ErrorContains(t, err, "invalid key")

	// common scenario - get the pod info
	podInfo, err = tracker.GetPodInfo("p*0")
	assert.Nilf(t, err, "error should be nil")
	assert.Equal(t, "p", podInfo.monoVertexName)
	assert.Equal(t, "p-mv-0", podInfo.podName)

	// common scenario - incorrect the pod info
	podInfo, err = tracker.GetPodInfo("p*avc")
	assert.Nilf(t, podInfo, "podInfo should be nil")
	assert.ErrorContains(t, err, "invalid replica in key")
}
