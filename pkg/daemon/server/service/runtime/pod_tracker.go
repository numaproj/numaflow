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
	activePodIndices    map[string][]int
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
		resolver:         poddiscovery.NewResolver(),
		activePodIndices: make(map[string][]int),
		// Default refresh interval for updating the active pod set
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
	// start updating active pods as soon as called and then after every refreshInterval
	pt.updateActivePods(ctx)
	// close the channel to signal first update
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

// updateActivePods discovers pod candidates and verifies which runtime endpoints are active.
func (pt *PodTracker) updateActivePods(ctx context.Context) {
	for _, v := range pt.pipeline.Spec.Vertices {
		vertexName := v.Name
		active, err := pt.discoverVertexIndices(ctx, vertexName)
		if err != nil {
			pt.log.Warnf("Failed to discover pods for vertex %s: %v; retaining its previous active pod set", vertexName, err)
			continue
		}
		pt.setActivePodIndices(vertexName, active)
	}
}

func (pt *PodTracker) discoverVertexIndices(ctx context.Context, vertexName string) ([]int, error) {
	return poddiscovery.ResolveAndProbe(
		ctx,
		pt.resolver,
		poddiscovery.PipelineVertexRequest(pt.pipeline.Name, vertexName, pt.pipeline.Namespace, v1alpha1.VertexRuntimePortName),
		func(index int) bool {
			podName := fmt.Sprintf("%s-%s-%d", pt.pipeline.Name, vertexName, index)
			return pt.isActive(vertexName, podName)
		},
	)
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

// setActivePodIndices replaces the active pod snapshot for a vertex.
func (pt *PodTracker) setActivePodIndices(vertexName string, indices []int) {
	pt.activePodsMutex.Lock()
	defer pt.activePodsMutex.Unlock()
	pt.log.Debugf("Setting active pod indices for vertex %s to %v", vertexName, indices)
	pt.activePodIndices[vertexName] = append([]int(nil), indices...)
}

// GetActivePodIndicesForVertex returns a copy of the active replica indices for a vertex.
func (pt *PodTracker) GetActivePodIndicesForVertex(vertexName string) []int {
	pt.activePodsMutex.RLock()
	defer pt.activePodsMutex.RUnlock()
	return append([]int(nil), pt.activePodIndices[vertexName]...)
}
