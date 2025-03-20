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

const runtimeErrorsPath = "runtime/errors"
const runtimeErrorsTimeStep = 60 * time.Second

// MonoVertexRuntimeCache is an interface for caching and retrieving the runtime information.
type MonoVertexRuntimeCache interface {
	// StartCacheRefresher starts the cache refresher to update the local cache with the runtime errors.
	StartCacheRefresher(ctx context.Context) error
	// GetLocalCache returns the local cache of runtime errors.
	GetLocalCache() map[PodReplica][]ErrorDetails
}

var _ MonoVertexRuntimeCache = (*Runtime)(nil)

type PodReplica string

type RuntimeErrorApiResponse struct {
	ErrMssg string         `json:"error_message"`
	Data    []ErrorDetails `json:"data"`
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

type Runtime struct {
	monoVtx    *v1alpha1.MonoVertex
	localCache map[PodReplica][]ErrorDetails
	cacheMutex sync.Mutex
	podTracker *PodTracker
	log        *zap.SugaredLogger
	httpClient monitorHttpClient
}

func NewRuntime(ctx context.Context, mv *v1alpha1.MonoVertex) *Runtime {
	runtime := Runtime{
		monoVtx:    mv,
		localCache: make(map[PodReplica][]ErrorDetails),
		cacheMutex: sync.Mutex{},
		log:        logging.FromContext(ctx).Named("Runtime"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 1,
		},
	}
	runtime.podTracker = NewPodTracker(ctx, mv)
	return &runtime
}

func (r *Runtime) StartCacheRefresher(ctx context.Context) (err error) {
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
func (r *Runtime) persistRuntimeErrors(ctx context.Context) {
	fetchAndPersistErrors := func() {
		var wg sync.WaitGroup

		for i := range r.podTracker.GetActivePodsCount() {
			wg.Add(1)

			go func(podIndex int) {
				defer wg.Done()

				// Get the headless service name
				url := fmt.Sprintf("https://%s-mv-%v.%s.%s.svc:%v/%s", r.monoVtx.Name, podIndex, r.monoVtx.GetHeadlessServiceName(), r.monoVtx.GetNamespace(), v1alpha1.MonoVertexMonitorPort, runtimeErrorsPath)

				res, err := r.httpClient.Get(url)
				if err != nil {
					r.log.Errorw("Error reading the runtime errors endpoint", "error", err.Error())
					return
				}
				defer res.Body.Close()

				// Read the response body
				body, err := io.ReadAll(res.Body)
				if err != nil {
					r.log.Errorf("Error reading response body from %s: %v", url, err)
					return
				}

				// Parse the response body into runtime api response
				var apiResponse RuntimeErrorApiResponse
				if err := json.Unmarshal(body, &apiResponse); err != nil {
					r.log.Errorf("Error decoding runtime error response from %s: %v", url, err)
					return
				}

				if apiResponse.ErrMssg != "" {
					return
				}

				cacheKey := PodReplica(fmt.Sprintf("%s-mv-%v", r.monoVtx.Name, podIndex))

				// Lock the cache before updating
				r.cacheMutex.Lock()
				defer r.cacheMutex.Unlock()

				_, ok := r.localCache[cacheKey]
				if !ok {
					r.localCache[cacheKey] = make([]ErrorDetails, 0)
				}
				r.log.Infof("Persisting error in local cache for: %s", cacheKey)
				// overwrite the errors
				r.localCache[cacheKey] = apiResponse.Data
			}(i)
		}

		// Wait for all goroutines to finish
		wg.Wait()
	}

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

func (r *Runtime) GetLocalCache() map[PodReplica][]ErrorDetails {
	return r.localCache
}
