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

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"
)

const runtimeErrorsPath = "runtime/errors"
const runtimeErrorsTimeStep = 60 * time.Second

type MonoVtxRuntime interface {
	Start(ctx context.Context) error
	PersistRuntimeErrors(ctx context.Context)
}

type PodReplica string

type RuntimeErrorApiResponse struct {
	ErrMssg string         `json:"err_mssg"`
	Errors  []ErrorDetails `json:"errors"`
}
type ErrorDetails struct {
	Container string `json:"container_name"`
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

func (r *Runtime) Start(ctx context.Context) (err error) {
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
	go r.PersistRuntimeErrors(ctx)

	return nil
}

// PersistRuntimeErrors updates the local cache with the runtime errors
func (r *Runtime) PersistRuntimeErrors(ctx context.Context) {
	ticker := time.NewTicker(runtimeErrorsTimeStep)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:

			for i := range r.podTracker.GetActivePodIndexes() {
				// Get the headless service name
				// We can query the runtime errors endpoint of the (i)th pod to obtain this value.
				// example for 0th pod : https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless:2470/runtime/errors
				url := fmt.Sprintf("https://%s-mv-%v.%s.%s.svc:%v/%s", r.monoVtx.Name, i, r.monoVtx.GetHeadlessServiceName(), r.monoVtx.GetNamespace(), v1alpha1.MonoVertexMonitorPort, runtimeErrorsPath)

				if res, err := r.httpClient.Get(url); err != nil {
					r.log.Errorw("Error reading the runtime errors endpoint: %f", err.Error())
					continue
				} else {
					// Read the response body
					body, err := io.ReadAll(res.Body)
					res.Body.Close()
					if err != nil {
						r.log.Errorf("Error reading response body from %s: %v", url, err)
						continue
					}

					// Parse the response body into runtime api response
					var apiResponse RuntimeErrorApiResponse
					if err := json.Unmarshal(body, &apiResponse); err != nil {
						r.log.Errorf("Error decoding runtime error response from %s: %v", url, err)
						continue
					}

					if apiResponse.ErrMssg != "" {
						continue
					}

					cacheKey := PodReplica(fmt.Sprintf("%s-mv-%v", r.monoVtx.Name, i))

					// Lock the cache before updating
					r.cacheMutex.Lock()
					_, ok := r.localCache[cacheKey]
					if !ok {
						r.localCache[cacheKey] = make([]ErrorDetails, 0)
					}
					r.log.Infof("Persisting error in local cache for: %s", cacheKey)
					// overwrite the errors - if errors are gone, we should update the local cache to empty
					// max 10 files for a container to be checked by write flow
					r.localCache[cacheKey] = apiResponse.Errors
					r.cacheMutex.Unlock()
				}
			}
		// If the context is done, return.
		case <-ctx.Done():
			r.log.Info("Context canceled, stopping PersistRuntimeErrors")
			return
		}
	}
}
