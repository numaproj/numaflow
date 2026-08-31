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
	runtimeErrorsPath            = "runtime/errors"
	defaultRuntimeErrorsTimeStep = 10 * time.Second
)

// ErrorDetails is used to provide information for a container error
// including timestamp, error code, message and details
type ErrorDetails struct {
	Container string `json:"container"`
	Timestamp int64  `json:"timestamp"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Details   string `json:"details"`
}

type ErrorApiResponse struct {
	ErrMessage string         `json:"errorMessage"`
	Data       []ErrorDetails `json:"data"`
}

// ReplicaErrors is used to provide all the container errors for a replica
type ReplicaErrors struct {
	Replica         string         `json:"replica"`
	ContainerErrors []ErrorDetails `json:"containerErrors"`
}

// MonoVertexRuntimeCache is an interface for caching and retrieving the runtime information.
type MonoVertexRuntimeCache interface {
	// StartCacheRefresher starts the cache refresher to update the local cache with the runtime errors.
	StartCacheRefresher(ctx context.Context) error
	// GetLocalCache returns the local cache of runtime errors for each monoVertex.
	GetLocalCache() map[string][]ReplicaErrors
}

var _ MonoVertexRuntimeCache = (*monoVertexRuntimeCache)(nil)

type runtimeHTTPClient interface {
	Get(url string) (*http.Response, error)
	Head(url string) (*http.Response, error)
}

// RuntimeOption configures a monoVertexRuntimeCache.
type RuntimeOption func(*monoVertexRuntimeCache)

// WithRuntimeErrorsTimeStep sets how often runtime errors are refreshed from active pods.
func WithRuntimeErrorsTimeStep(d time.Duration) RuntimeOption {
	return func(r *monoVertexRuntimeCache) {
		r.runtimeErrorsTimeStep = d
	}
}

// WithPodTrackerOptions configures the pod tracker used by the runtime cache.
func WithPodTrackerOptions(opts ...PodTrackerOption) RuntimeOption {
	return func(r *monoVertexRuntimeCache) {
		r.podTrackerOpts = append(r.podTrackerOpts, opts...)
	}
}

// WithRuntimeHTTPClient sets the HTTP client used to fetch runtime errors from worker pods.
func WithRuntimeHTTPClient(client runtimeHTTPClient) RuntimeOption {
	return func(r *monoVertexRuntimeCache) {
		r.httpClient = client
	}
}

// monoVertexRuntimeCache is used to store the local cache of runtime errors for a monoVertex.
// It implements the MonoVertexRuntimeCache interface.
type monoVertexRuntimeCache struct {
	monoVtx               *v1alpha1.MonoVertex
	localCache            map[string][]ReplicaErrors
	cacheMutex            sync.RWMutex
	podTracker            *PodTracker
	log                   *zap.SugaredLogger
	httpClient            runtimeHTTPClient
	runtimeErrorsTimeStep time.Duration
	podTrackerOpts        []PodTrackerOption
}

// NewRuntime creates a new instance of monoVertexRuntimeCache.
func NewRuntime(ctx context.Context, mv *v1alpha1.MonoVertex, opts ...RuntimeOption) MonoVertexRuntimeCache {
	r := &monoVertexRuntimeCache{
		monoVtx:    mv,
		localCache: make(map[string][]ReplicaErrors),
		cacheMutex: sync.RWMutex{},
		log:        logging.FromContext(ctx).Named("monoVertexRuntimeCache"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 1,
		},
		runtimeErrorsTimeStep: defaultRuntimeErrorsTimeStep,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	r.podTracker = NewPodTracker(ctx, mv, r.podTrackerOpts...)
	return r
}

// StartCacheRefresher starts the cache refresher to update the local cache periodically with the runtime errors.
func (r *monoVertexRuntimeCache) StartCacheRefresher(ctx context.Context) (err error) {
	r.log.Infof("Starting runtime cache refresher...")
	ctx, cancel := context.WithCancel(logging.WithLogger(ctx, r.log))
	go func() {
		err := r.podTracker.Start(ctx)
		if err != nil {
			r.log.Errorw("Failed to start pod tracker for runtime cache refresher", zap.Error(err))
			cancel()
		}
	}()
	// wait for first active pods update for MonoVertex
	<-r.podTracker.firstPodsUpdateChan
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
	ticker := time.NewTicker(r.runtimeErrorsTimeStep)
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
	url := fmt.Sprintf("https://%s-mv-%v.%s.%s.svc:%v/%s", r.monoVtx.Name, podIndex, r.monoVtx.GetHeadlessServiceName(), r.monoVtx.GetNamespace(), v1alpha1.MonoVertexRuntimePort, runtimeErrorsPath)

	res, err := r.httpClient.Get(url)
	if err != nil {
		r.log.Debugf("[MonoVertex %s Index %v]: failed reading the runtime endpoint, the pod might have been scaled down: %v", r.monoVtx.Name, podIndex, err.Error())
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

	// return if data array length is 0 or if there is any error in the API call
	if apiResponse.ErrMessage != "" || len(apiResponse.Data) == 0 {
		return
	}

	cacheKey := r.monoVtx.Name
	replica := fmt.Sprintf("%s-mv-%v", cacheKey, podIndex)
	newMvtxErrors := ReplicaErrors{Replica: replica, ContainerErrors: apiResponse.Data}

	r.log.Debugf("Persisting error in local cache for: %s", cacheKey)
	// Lock the cache before updating
	r.cacheMutex.Lock()
	defer r.cacheMutex.Unlock()

	// overwrite the errors
	if mvtxErrors, ok := r.localCache[cacheKey]; ok {
		// Update existing replica errors if found
		for i, mvtxError := range mvtxErrors {
			if mvtxError.Replica == replica {
				mvtxErrors[i] = newMvtxErrors
				r.localCache[cacheKey] = mvtxErrors
				return
			}
		}
		// Append new replica errors if not found
		r.localCache[cacheKey] = append(mvtxErrors, newMvtxErrors)
	} else {
		// Initialize the cache entry with the new replica errors
		r.localCache[cacheKey] = []ReplicaErrors{newMvtxErrors}
	}
}

// GetLocalCache returns the local cache of runtime errors.
func (r *monoVertexRuntimeCache) GetLocalCache() map[string][]ReplicaErrors {
	r.cacheMutex.RLock()
	defer r.cacheMutex.RUnlock()

	localCacheCopy := make(map[string][]ReplicaErrors, len(r.localCache))
	for key, value := range r.localCache {
		localCacheValue := make([]ReplicaErrors, len(value))
		for i, replicaErrors := range value {
			containerErrors := make([]ErrorDetails, len(replicaErrors.ContainerErrors))
			copy(containerErrors, replicaErrors.ContainerErrors)
			localCacheValue[i] = ReplicaErrors{
				Replica:         replicaErrors.Replica,
				ContainerErrors: containerErrors,
			}
		}
		localCacheCopy[key] = localCacheValue
	}
	return localCacheCopy
}
