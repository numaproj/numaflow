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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

const (
	runtimeErrorsPath     = "runtime/errors"
	runtimeErrorsTimeStep = 60 * time.Second
)

// MonoVertexRuntimeCache is an interface for caching and retrieving the runtime information.
type MonoVertexRuntimeCache interface {
	// StartCacheRefresher starts the cache refresher to update the local cache with the runtime errors.
	StartCacheRefresher(ctx context.Context) error
	// GetLocalCache returns the local cache of runtime errors.
	GetLocalCache() map[PodReplica][]ErrorDetails
}

var _ MonoVertexRuntimeCache = (*monoVertexRuntimeCache)(nil)

type PodReplica string

type ErrorApiResponse struct {
	ErrMessage string         `json:"error_message"`
	Data       []ErrorDetails `json:"data"`
}
type ErrorDetails struct {
	Container string `json:"container"`
	Timestamp string `json:"timestamp"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Details   string `json:"details"`
}

type monitorHttpClient interface {
	Get(url string) (*http.Response, error)
	Head(url string) (*http.Response, error)
}

// monoVertexRuntimeCache is used to store the local cache of runtime errors for a monoVertex.
// It implements the MonoVertexRuntimeCache interface.
type monoVertexRuntimeCache struct {
	monoVtx    *v1alpha1.MonoVertex
	localCache map[PodReplica][]ErrorDetails
	cacheMutex sync.RWMutex
	podTracker *PodTracker
	log        *zap.SugaredLogger
	httpClient monitorHttpClient
}

// NewRuntime creates a new instance of monoVertexRuntimeCache.
func NewRuntime(ctx context.Context, mv *v1alpha1.MonoVertex) MonoVertexRuntimeCache {
	return &monoVertexRuntimeCache{
		monoVtx:    mv,
		localCache: make(map[PodReplica][]ErrorDetails),
		cacheMutex: sync.RWMutex{},
		log:        logging.FromContext(ctx).Named("monoVertexRuntimeCache"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 1,
		},
		podTracker: NewPodTracker(ctx, mv),
	}
}

// StartCacheRefresher starts the cache refresher to update the local cache periodically with the runtime errors.
func (r *monoVertexRuntimeCache) StartCacheRefresher(ctx context.Context) (err error) {
	r.log.Infof("Starting runtime server...")

	ctx, cancel := context.WithCancel(logging.WithLogger(ctx, r.log))

	go func() {
		err := r.podTracker.Start(ctx)
		if err != nil {
			r.log.Errorw("Failed to start pod tracker for runtime server", zap.Error(err))
			cancel()
		}
	}()

	// start persisting errors into the local cache
	go r.persistRuntimeErrors(ctx)

	return nil
}

// persistRuntimeErrors updates the local cache with the runtime errors
func (r *monoVertexRuntimeCache) persistRuntimeErrors(ctx context.Context) {
	fetchAndPersistErrors := func() {
		var wg sync.WaitGroup

		for i := range r.podTracker.GetActivePodsCount() {
			wg.Add(1)
			go func(podIndex int) {
				defer wg.Done()
				r.fetchAndPersistErrorForPod(podIndex)
			}(i)
		}

		// Wait for all goroutines to finish
		wg.Wait()
	}

	// invoke once and then periodically update the cache
	fetchAndPersistErrors()

	// Set up a ticker to run periodically
	ticker := time.NewTicker(runtimeErrorsTimeStep)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fetchAndPersistErrors()
		// If the context is done, return.
		case <-ctx.Done():
			r.log.Info("Context canceled, stopping PersistRuntimeErrors")
			return
		}
	}
}

// fetchAndPersistErrorForPod fetches the runtime errors for a pod and persists them in the local cache.
func (r *monoVertexRuntimeCache) fetchAndPersistErrorForPod(podIndex int) {
	// Get the headless service name
	url := fmt.Sprintf("https://%s-mv-%v.%s.%s.svc:%v/%s", r.monoVtx.Name, podIndex, r.monoVtx.GetHeadlessServiceName(), r.monoVtx.GetNamespace(), v1alpha1.MonoVertexMonitorPort, runtimeErrorsPath)

	res, err := r.httpClient.Get(url)
	if err != nil {
		r.log.Errorw("Error reading the runtime errors endpoint", "error", err.Error())
		return
	}

	// Read the response body
	body, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil {
		r.log.Errorf("Error reading response body from %s: %v", url, err)
		return
	}

	// Parse the response body into runtime api response
	var apiResponse ErrorApiResponse
	if err := json.Unmarshal(body, &apiResponse); err != nil {
		r.log.Errorf("Error decoding runtime error response from %s: %v", url, err)
		return
	}

	if apiResponse.ErrMessage != "" {
		return
	}

	cacheKey := PodReplica(fmt.Sprintf("%s-mv-%v", r.monoVtx.Name, podIndex))

	// Lock the cache before updating
	r.cacheMutex.Lock()
	defer r.cacheMutex.Unlock()

	// overwrite the errors
	r.localCache[cacheKey] = apiResponse.Data
	r.log.Infof("Persisting error in local cache for: %s", cacheKey)
}

// GetLocalCache returns the local cache of runtime errors.
func (r *monoVertexRuntimeCache) GetLocalCache() map[PodReplica][]ErrorDetails {
	r.cacheMutex.RLock()
	defer r.cacheMutex.RUnlock()

	localCacheCopy := make(map[PodReplica][]ErrorDetails, len(r.localCache))
	for key, value := range r.localCache {
		localCacheValue := make([]ErrorDetails, len(value))
		copy(localCacheValue, value)
		localCacheCopy[key] = localCacheValue
	}
	return localCacheCopy
}
