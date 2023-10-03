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

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
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

	assert.Equal(t, "p*v*0*other", tracker.LeastRecentlyUsed())
	assert.Equal(t, "p*v*1*other", tracker.LeastRecentlyUsed())
	assert.Equal(t, true, tracker.IsActive("p*v*4*other"))
	assert.Equal(t, false, tracker.IsActive("p*v*5*other"))
}
