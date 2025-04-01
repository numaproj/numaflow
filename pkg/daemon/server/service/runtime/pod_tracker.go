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
)

// PodTracker tracks the active pods for each vertex in a pipeline.
type PodTracker struct {
	pipeline        *v1alpha1.Pipeline
	log             *zap.SugaredLogger
	httpClient      monitorHttpClient
	activePodsCount map[string]int
	activePodsMutex sync.RWMutex
	refreshInterval time.Duration
}

// NewPodTracker creates a new pod tracker instance.
func NewPodTracker(ctx context.Context, pl *v1alpha1.Pipeline) *PodTracker {
	pt := &PodTracker{
		pipeline: pl,
		log:      logging.FromContext(ctx).Named("RuntimePodTracker"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second,
		},
		activePodsCount: make(map[string]int),
		// Default refresh interval for updating the active pod set
		refreshInterval: 30 * time.Second,
	}
	return pt
}

// Start starts the pod tracker to track the active pods for the pipeline.
func (pt *PodTracker) Start(ctx context.Context) error {
	pt.log.Debugf("Starting tracking active pods for Pipeline %s...", pt.pipeline.Name)
	go pt.trackActivePods(ctx)
	return nil
}

func (pt *PodTracker) trackActivePods(ctx context.Context) {
	// start updating active pods as soon as called and then after every refreshInterval
	pt.updateActivePods()
	ticker := time.NewTicker(pt.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			pt.log.Infof("Context is cancelled. Stopping tracking active pods for pipeline %s...", pt.pipeline.Name)
			return
		case <-ticker.C:
			pt.updateActivePods()
		}
	}
}

// updateActivePods checks the status of all pods and updates the count of activePods accordingly.
func (pt *PodTracker) updateActivePods() {
	var wg sync.WaitGroup
	// Map to store max active index for each vertex.
	maxActiveIndex := make(map[string]int)

	pt.activePodsMutex.Lock()
	for _, v := range pt.pipeline.Spec.Vertices {
		vertexName := v.Name
		// Initialize maxActiveIndex for this vertex.
		maxActiveIndex[vertexName] = -1
		for i := range int(v.Scale.GetMaxReplicas()) {
			wg.Add(1)
			go func(vertexName string, index int) {
				defer wg.Done()
				podName := fmt.Sprintf("%s-%s-%d", pt.pipeline.Name, vertexName, index)
				if pt.isActive(vertexName, podName) {
					pt.updateMaxActiveIndex(vertexName, index, maxActiveIndex)
				}
			}(vertexName, i)
		}
	}
	pt.activePodsMutex.Unlock()
	wg.Wait()

	// Update the activePodsCount for all vertices.
	for vertexName, maxIndex := range maxActiveIndex {
		pt.setActivePodsCount(vertexName, maxIndex+1)
	}
}

func (pt *PodTracker) isActive(vertexName, podName string) bool {
	// example for 0th pod: https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc:2470/runtime/errors
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/runtime/errors", podName, pt.pipeline.Name+"-"+vertexName+"-headless", pt.pipeline.Namespace, v1alpha1.VertexMonitorPort)
	resp, err := pt.httpClient.Head(url)
	if err != nil {
		pt.log.Debugf("Sending HEAD request to pod %s is unsuccessful: %v, treating the pod as inactive", podName, err)
		return false
	}
	pt.log.Debugf("Sending HEAD request to pod %s is successful, treating the pod as active", podName)
	_ = resp.Body.Close()
	return true
}

// updateMaxActiveIndex updates the maximum active pod index for a vertex.
func (pt *PodTracker) updateMaxActiveIndex(vertexName string, index int, maxActiveIndex map[string]int) {
	pt.activePodsMutex.Lock()
	defer pt.activePodsMutex.Unlock()

	if index > maxActiveIndex[vertexName] {
		maxActiveIndex[vertexName] = index
	}
}

// setActivePodsCount sets the activePodsCount for a vertex.
func (pt *PodTracker) setActivePodsCount(vertexName string, count int) {
	pt.activePodsMutex.Lock()
	defer pt.activePodsMutex.Unlock()
	pt.activePodsCount[vertexName] = count
}

// GetActivePodsCountForVertex returns the number of active pods for a vertex
func (pt *PodTracker) GetActivePodsCountForVertex(vertexName string) int {
	pt.activePodsMutex.RLock()
	defer pt.activePodsMutex.RUnlock()
	return pt.activePodsCount[vertexName]
}
