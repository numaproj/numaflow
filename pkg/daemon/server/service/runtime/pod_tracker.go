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

package runtime

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/poddiscovery"
)

// PodTracker tracks the active pods for each vertex in a pipeline.
type PodTracker struct {
	pipeline            *v1alpha1.Pipeline
	log                 *zap.SugaredLogger
	httpClient          runtimeHTTPClient
	resolver            poddiscovery.Resolver
	activePodsCount     map[string]int
	activePodsMutex     sync.RWMutex
	refreshInterval     time.Duration
	firstPodsUpdateChan chan struct{} // Channel to signal the first active pods update is done
}

type PodTrackerOption func(*PodTracker)

// NewPodTracker creates a new pod tracker instance.
func NewPodTracker(ctx context.Context, pl *v1alpha1.Pipeline, opts ...PodTrackerOption) *PodTracker {
	pt := &PodTracker{
		pipeline: pl,
		log:      logging.FromContext(ctx).Named("RuntimePodTracker"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second,
		},
		resolver:            poddiscovery.NewResolver(),
		activePodsCount:     make(map[string]int),
		refreshInterval:     30 * time.Second,
		firstPodsUpdateChan: make(chan struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(pt)
		}
	}
	return pt
}

// WithPodResolver sets the resolver used to discover indexed pods.
func WithPodResolver(resolver poddiscovery.Resolver) PodTrackerOption {
	return func(pt *PodTracker) {
		pt.resolver = resolver
	}
}

// Start starts the pod tracker to track the active pods for the pipeline.
func (pt *PodTracker) Start(ctx context.Context) error {
	pt.log.Debugf("Starting tracking active pods for Pipeline %s...", pt.pipeline.Name)
	go pt.trackActivePods(ctx)
	return nil
}

func (pt *PodTracker) trackActivePods(ctx context.Context) {
	pt.updateActivePods(ctx)
	close(pt.firstPodsUpdateChan)
	ticker := time.NewTicker(pt.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			pt.log.Infof("Context is cancelled. Stopping tracking active pods for pipeline %s...", pt.pipeline.Name)
			return
		case <-ticker.C:
			pt.updateActivePods(ctx)
		}
	}
}

// updateActivePods discovers replicas from DNS and verifies which runtime endpoints are active.
func (pt *PodTracker) updateActivePods(ctx context.Context) {
	for _, v := range pt.pipeline.Spec.Vertices {
		vertexName := v.Name
		indices, err := pt.resolver.Resolve(ctx, poddiscovery.PipelineVertexRequest(
			pt.pipeline.Name, vertexName, pt.pipeline.Namespace,
		))
		if err != nil {
			pt.log.Warnf("Failed to discover pods for vertex %s: %v; retaining its previous active pod count", vertexName, err)
			continue
		}

		var wg sync.WaitGroup
		var maxActiveIndex atomic.Int32
		maxActiveIndex.Store(int32(-1))
		for _, index := range indices {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				podName := fmt.Sprintf("%s-%s-%d", pt.pipeline.Name, vertexName, index)
				if pt.isActive(vertexName, podName) {
					for {
						currentMax := maxActiveIndex.Load()
						if int32(index) > currentMax {
							if maxActiveIndex.CompareAndSwap(currentMax, int32(index)) {
								break
							}
						} else {
							break
						}
					}
				}
			}(index)
		}
		wg.Wait()

		count := int(maxActiveIndex.Load() + 1)
		pt.setActivePodsCount(vertexName, count)
		pt.log.Debugf("Finished updating runtime active pod count for vertex %s: %d", vertexName, count)
	}
}

func (pt *PodTracker) isActive(vertexName, podName string) bool {
	// example for 0th pod: https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc:2470/runtime/errors
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/runtime/errors", podName, pt.pipeline.Name+"-"+vertexName+"-headless", pt.pipeline.Namespace, v1alpha1.VertexRuntimePort)
	resp, err := pt.httpClient.Head(url)
	if err != nil {
		pt.log.Debugf("Sending HEAD request to pod %s is unsuccessful: %v, treating the pod as inactive", podName, err)
		return false
	}
	pt.log.Debugf("Sending HEAD request to pod %s is successful, treating the pod as active", podName)
	_ = resp.Body.Close()
	return true
}

func (pt *PodTracker) setActivePodsCount(vertexName string, count int) {
	pt.activePodsMutex.Lock()
	defer pt.activePodsMutex.Unlock()
	pt.log.Debugf("Setting active pods count for vertex %s to %d", vertexName, count)
	pt.activePodsCount[vertexName] = count
}

// GetActivePodsCountForVertex returns the number of active pods for a vertex.
func (pt *PodTracker) GetActivePodsCountForVertex(vertexName string) int {
	pt.activePodsMutex.RLock()
	defer pt.activePodsMutex.RUnlock()
	return pt.activePodsCount[vertexName]
}
