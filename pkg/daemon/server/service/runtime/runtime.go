package runtime

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"go.uber.org/zap"
)

const runtimeErrorsPath = "runtime/errors"
const runtimeErrorsTimeStep = 60 * time.Second

type PipelineRuntime interface {
	Start(ctx context.Context) error
	PersistRuntimeErrors(ctx context.Context)
	GetLocalCache() map[PodReplica][]ErrorDetails
}

var _ PipelineRuntime = (*Runtime)(nil)

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
	pipeline   *v1alpha1.Pipeline
	localCache map[PodReplica][]ErrorDetails
	cacheMutex sync.Mutex
	podTracker *PodTracker
	log        *zap.SugaredLogger
	httpClient monitorHttpClient
}

func NewRuntime(ctx context.Context, pl *v1alpha1.Pipeline) *Runtime {
	runtime := Runtime{
		pipeline:   pl,
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
	runtime.podTracker = NewPodTracker(ctx, pl)
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
			for _, vtx := range r.pipeline.Spec.Vertices {
				for i := range r.podTracker.GetActivePodIndexes(vtx) {
					podName := strings.Join([]string{r.pipeline.Name, vtx.Name, fmt.Sprintf("%d", i)}, "-")
					// example for 0th pod : https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc:2470/runtime/errors
					url := fmt.Sprintf("https://%s.%s.%s.svc:%v/%s", podName, r.pipeline.Name+"-"+vtx.Name+"-headless", r.pipeline.Namespace, v1alpha1.VertexMonitorPort, runtimeErrorsPath)
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

						cacheKey := PodReplica(fmt.Sprintf("%s-%s-%v", r.pipeline.Name, vtx.Name, i))

						// Lock the cache before updating
						r.cacheMutex.Lock()
						_, ok := r.localCache[cacheKey]
						if !ok {
							r.localCache[cacheKey] = make([]ErrorDetails, 0)
						}
						r.log.Infof("Persisting error in local cache for: %s", cacheKey)
						// overwrite the errors - if errors are gone, we should update the local cache to empty
						// max 10 files for a container to be checked by write flow
						r.localCache[cacheKey] = apiResponse.Data
						r.cacheMutex.Unlock()
					}
				}
			}
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
