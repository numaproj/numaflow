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

// PodTracker tracks the active pods for a MonoVertex.
type PodTracker struct {
	monoVertex          *v1alpha1.MonoVertex
	log                 *zap.SugaredLogger
	httpClient          runtimeHTTPClient
	resolver            poddiscovery.Resolver
	activePodIndices    []int
	activePodsMutex     sync.RWMutex
	refreshInterval     time.Duration
	firstPodsUpdateChan chan struct{} // Channel to signal the first active pods update is done
}

type PodTrackerOption func(*PodTracker)

// NewPodTracker creates a new pod tracker instance.
func NewPodTracker(ctx context.Context, mv *v1alpha1.MonoVertex, opts ...PodTrackerOption) *PodTracker {
	pt := &PodTracker{
		monoVertex: mv,
		log:        logging.FromContext(ctx).Named("RuntimePodTracker"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second,
		},
		resolver:            poddiscovery.NewResolver(),
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

// Start starts the pod tracker to track the active pods for the MonoVertex.
func (pt *PodTracker) Start(ctx context.Context) error {
	pt.log.Debugf("Starting tracking active pods for MonoVertex %s...", pt.monoVertex.Name)
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
			pt.log.Infof("Context is cancelled. Stopping tracking active pods for MonoVertex %s...", pt.monoVertex.Name)
			return
		case <-ticker.C:
			pt.updateActivePods(ctx)
		}
	}
}

// updateActivePods discovers pod candidates and verifies which runtime endpoints are active.
func (pt *PodTracker) updateActivePods(ctx context.Context) {
	active, err := poddiscovery.ResolveAndProbe(
		ctx,
		pt.resolver,
		poddiscovery.MonoVertexRequest(pt.monoVertex.Name, pt.monoVertex.Namespace, v1alpha1.MonoVertexRuntimePortName),
		func(index int) bool {
			return pt.isActive(fmt.Sprintf("%s-mv-%d", pt.monoVertex.Name, index))
		},
	)
	if err != nil {
		pt.log.Warnf("Failed to discover MonoVertex pods: %v; retaining the previous active pod set", err)
		return
	}
	pt.setActivePodIndices(active)
}

func (pt *PodTracker) isActive(podName string) bool {
	headlessSvc := pt.monoVertex.GetHeadlessServiceName()
	// example for 0th pod: https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless.default.svc:2470/runtime/errors
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/runtime/errors", podName, headlessSvc, pt.monoVertex.Namespace, v1alpha1.MonoVertexRuntimePort)
	resp, err := pt.httpClient.Head(url)
	if err != nil {
		pt.log.Debugf("Sending HEAD request to pod %s is unsuccessful: %v, treating the pod as inactive", podName, err)
		return false
	}
	pt.log.Debugf("Sending HEAD request to pod %s is successful, treating the pod as active", podName)
	_ = resp.Body.Close()
	return true
}

// setActivePodIndices replaces the active pod snapshot.
func (pt *PodTracker) setActivePodIndices(indices []int) {
	pt.activePodsMutex.Lock()
	defer pt.activePodsMutex.Unlock()
	pt.log.Debugf("Setting active pod indices to %v", indices)
	pt.activePodIndices = append([]int(nil), indices...)
}

// GetActivePodIndices returns a copy of the active replica indices.
func (pt *PodTracker) GetActivePodIndices() []int {
	pt.activePodsMutex.RLock()
	defer pt.activePodsMutex.RUnlock()
	return append([]int(nil), pt.activePodIndices...)
}
