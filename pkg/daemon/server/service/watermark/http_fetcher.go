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

package watermark

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// HTTPClient interface for making HTTP requests (for testing)
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Response represents the JSON response from the /runtime/watermark endpoint
// Example: {"0": 1752314641078, "1": 1752314641079}
type Response struct {
	Partitions map[string]int64 `json:"-"` // Use custom unmarshaling
}

// UnmarshalJSON implements custom JSON unmarshaling for WatermarkResponse
func (w *Response) UnmarshalJSON(data []byte) error {
	w.Partitions = make(map[string]int64)
	return json.Unmarshal(data, &w.Partitions)
}

// HTTPWatermarkFetcher fetches watermarks from vertex pods via HTTP /runtime/watermark endpoint
type HTTPWatermarkFetcher struct {
	pipeline   *v1alpha1.Pipeline
	edge       v1alpha1.Edge
	httpClient HTTPClient
	log        *zap.SugaredLogger

	// Cache for storing watermarks - serves requests from this cache
	watermarkCache map[int]int64 // partition -> watermark
	cacheMutex     sync.RWMutex

	// Background fetching control
	ctx         context.Context
	cancel      context.CancelFunc
	fetchTicker *time.Ticker
}

// NewHTTPWatermarkFetcher creates a new HTTP-based watermark fetcher and starts background fetching
func NewHTTPWatermarkFetcher(ctx context.Context, pipeline *v1alpha1.Pipeline, edge v1alpha1.Edge) *HTTPWatermarkFetcher {
	log := logging.FromContext(ctx).With("edge", edge.GetEdgeName())

	fetcherCtx, cancel := context.WithCancel(ctx)

	fetcher := &HTTPWatermarkFetcher{
		pipeline: pipeline,
		edge:     edge,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		},
		log:            log,
		watermarkCache: make(map[int]int64),
		ctx:            fetcherCtx,
		cancel:         cancel,
		fetchTicker:    time.NewTicker(500 * time.Millisecond),
	}

	// Initialize cache with -1 values
	toVertex := pipeline.GetVertex(edge.To)
	if toVertex != nil {
		partitionCount := toVertex.GetPartitionCount()
		for i := 0; i < partitionCount; i++ {
			fetcher.watermarkCache[i] = -1
		}
	}

	// Start background fetching
	go fetcher.startBackgroundFetching()

	log.Info("Started HTTP watermark fetcher with background fetching")
	return fetcher
}

// GetWatermarks returns cached watermarks for all partitions of the edge
func (h *HTTPWatermarkFetcher) GetWatermarks() ([]*wrapperspb.Int64Value, error) {
	toVertex := h.pipeline.GetVertex(h.edge.To)
	if toVertex == nil {
		return nil, fmt.Errorf("vertex %s not found", h.edge.To)
	}

	partitionCount := toVertex.GetPartitionCount()
	return h.getCachedWatermarks(partitionCount), nil
}

// startBackgroundFetching runs the background watermark fetching loop
func (h *HTTPWatermarkFetcher) startBackgroundFetching() {
	h.log.Debug("Starting background watermark fetching")

	for {
		select {
		case <-h.ctx.Done():
			h.log.Info("Stopping background watermark fetching")
			h.fetchTicker.Stop()
			return
		case <-h.fetchTicker.C:
			h.fetchAndUpdateCache()
		}
	}
}

// fetchAndUpdateCache fetches watermarks and updates the cache
func (h *HTTPWatermarkFetcher) fetchAndUpdateCache() {
	toVertex := h.pipeline.GetVertex(h.edge.To)
	if toVertex == nil {
		h.log.Errorf("Vertex %s not found", h.edge.To)
		return
	}

	partitionCount := toVertex.GetPartitionCount()
	isReduce := toVertex.IsReduceUDF()

	h.log.Debugf("Fetching watermarks for edge %s (reduce: %t, partitions: %d)",
		h.edge.GetEdgeName(), isReduce, partitionCount)

	if isReduce {
		// For reduce vertices: hit all pods (x partitions = x pods), 1:1 pod to partition mapping
		h.fetchFromAllPods(partitionCount)
	} else {
		// For non-reduce vertices: hit only 0th pod to get all partition watermarks, 1:n pod to partition mapping
		h.fetchFromSinglePod(partitionCount)
	}
}

// fetchFromSinglePod fetches watermarks from the 0th pod for non-reduce vertices
func (h *HTTPWatermarkFetcher) fetchFromSinglePod(partitionCount int) {
	ctx, cancel := context.WithTimeout(h.ctx, time.Second*3)
	defer cancel()

	toVertex := h.pipeline.GetVertex(h.edge.To)
	if toVertex == nil {
		h.log.Errorf("From vertex %s not found", h.edge.To)
		return
	}

	// Build URL for 0th pod of the to vertex with from query parameter
	headlessServiceName := fmt.Sprintf("%s-%s-headless", h.pipeline.Name, h.edge.To)
	podName := fmt.Sprintf("%s-%s-0", h.pipeline.Name, h.edge.To)
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/runtime/watermark?from=%s",
		podName, headlessServiceName, h.pipeline.Namespace, v1alpha1.VertexMetricsPort, h.edge.From)

	h.log.Debugf("Background fetching watermarks from single pod: %s", url)

	watermarkMap, err := h.fetchFromURL(ctx, url)
	if err != nil {
		h.log.Debugf("Failed to fetch watermarks from pod %s: %v", podName, err)
		return // Keep existing cache values
	}

	// Update cache with fetched watermarks
	for i := 0; i < partitionCount; i++ {
		partitionKey := fmt.Sprintf("%d", i)
		if wm, exists := watermarkMap[partitionKey]; exists && wm != -1 {
			h.updateCache(i, wm)
		}
		// If watermark is -1 or missing, keep existing cache value
	}
}

// fetchFromAllPods fetches watermarks from all pods for reduce vertices
func (h *HTTPWatermarkFetcher) fetchFromAllPods(partitionCount int) {
	toVertex := h.pipeline.GetVertex(h.edge.To)
	if toVertex == nil {
		h.log.Errorf("To vertex %s not found", h.edge.To)
		return
	}

	headlessServiceName := fmt.Sprintf("%s-%s-headless", h.pipeline.Name, h.edge.To)

	// For reduce vertices, each partition maps to a separate pod
	for i := 0; i < partitionCount; i++ {
		ctx, cancel := context.WithTimeout(h.ctx, time.Second*3)

		podName := fmt.Sprintf("%s-%s-%d", h.pipeline.Name, h.edge.To, i)
		url := fmt.Sprintf("https://%s.%s.%s.svc:%v/runtime/watermark?from=%s",
			podName, headlessServiceName, h.pipeline.Namespace, v1alpha1.VertexMetricsPort, h.edge.From)

		h.log.Debugf("Background fetching watermark from pod %d: %s", i, url)

		watermarkMap, err := h.fetchFromURL(ctx, url)
		cancel()

		if err != nil {
			h.log.Debugf("Failed to fetch watermark from pod %s: %v", podName, err)
			continue // Keep existing cache value for this partition
		}

		partitionKey := fmt.Sprintf("%d", i)
		// For reduce vertices, we expect only partition "0" in the response
		if wm, exists := watermarkMap[partitionKey]; exists && wm != -1 {
			h.updateCache(i, wm)
		}
		// If watermark is -1 or missing, keep existing cache value
	}
}

// fetchFromURL makes HTTP request to fetch watermark from a specific URL
func (h *HTTPWatermarkFetcher) fetchFromURL(ctx context.Context, url string) (map[string]int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status: %d", resp.StatusCode)
	}

	var watermarkResp Response
	if err := json.NewDecoder(resp.Body).Decode(&watermarkResp); err != nil {
		return nil, fmt.Errorf("failed to decode JSON response: %w", err)
	}

	return watermarkResp.Partitions, nil
}

// updateCache updates the watermark cache for a specific partition
func (h *HTTPWatermarkFetcher) updateCache(partition int, watermark int64) {
	h.cacheMutex.Lock()
	defer h.cacheMutex.Unlock()
	h.watermarkCache[partition] = watermark
}

// getCached retrieves cached watermark for a partition, returns -1 if not found
func (h *HTTPWatermarkFetcher) getCached(partition int) int64 {
	h.cacheMutex.RLock()
	defer h.cacheMutex.RUnlock()
	if wm, exists := h.watermarkCache[partition]; exists {
		return wm
	}
	return -1 // Default watermark value when no cache is available
}

// getCachedWatermarks returns cached watermarks for all partitions
func (h *HTTPWatermarkFetcher) getCachedWatermarks(partitionCount int) []*wrapperspb.Int64Value {
	watermarks := make([]*wrapperspb.Int64Value, partitionCount)
	for i := 0; i < partitionCount; i++ {
		cachedWm := h.getCached(i)
		watermarks[i] = wrapperspb.Int64(cachedWm)
	}
	return watermarks
}

// Stop stops the background fetching and cleans up resources
func (h *HTTPWatermarkFetcher) Stop() {
	h.log.Info("Stopping HTTP watermark fetcher")
	if h.cancel != nil {
		h.cancel()
	}
	if h.fetchTicker != nil {
		h.fetchTicker.Stop()
	}
}
