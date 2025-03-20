package runtime

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// podInfoSeparator is used as a separator to split the pod key
// to get the pipeline, vertex and pod index.
// "*" is chosen because it is not allowed in the above fields.
const podInfoSeparator = "*"

// PodTracker tracks the active pods for each vertex in a pipeline.
type PodTracker struct {
	pipeline        *v1alpha1.Pipeline
	log             *zap.SugaredLogger
	httpClient      monitorHttpClient
	activePods      *util.UniqueStringList
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
		activePods: util.NewUniqueStringList(),
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

// updateActivePods checks the status of all pods and updates the activePods set accordingly.
func (pt *PodTracker) updateActivePods() {
	var wg sync.WaitGroup
	for _, v := range pt.pipeline.Spec.Vertices {
		for i := range int(v.Scale.GetMaxReplicas()) {
			wg.Add(1)
			go func(vertexName string, index int) {
				defer wg.Done()
				podName := fmt.Sprintf("%s-%s-%d", pt.pipeline.Name, vertexName, index)
				podKey := pt.getPodKey(index, vertexName)
				if pt.isActive(vertexName, podName) {
					pt.activePods.PushBack(podKey)
				} else {
					pt.activePods.Remove(podKey)
				}
			}(v.Name, i)
		}
	}
	wg.Wait()
	pt.log.Debugf("Finished updating the active pod set: %v", pt.activePods.ToString())
}

func (pt *PodTracker) getPodKey(index int, vertexName string) string {
	// podKey is used as a unique identifier for the pod, which is a combination of vertex name and pod index.
	return strings.Join([]string{vertexName, fmt.Sprintf("%d", index)}, podInfoSeparator)
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

// GetActivePodsCountForVertex returns the number of active pods for a vertex
func (pt *PodTracker) GetActivePodsCountForVertex(vertexName string) int {
	count := 0
	values := pt.activePods.ToString()
	if values == "" {
		return count
	}
	// TODO: RETHINK
	podList := strings.Split(values, ",")
	for _, pod := range podList {
		parts := strings.Split(pod, podInfoSeparator)
		if len(parts) > 0 && parts[0] == vertexName {
			count++
		}
	}
	return count
}
