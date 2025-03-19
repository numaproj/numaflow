package runtime

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"go.uber.org/zap"
)

// podInfoSeparator is used as a separator to split the pod key
// to get the pipeline, vertex and pod index.
// "*" is chosen because it is not allowed in the above fields.
const podInfoSeparator = "*"

type PodTracker interface {
	Start(ctx context.Context) error
	GetActivePodIndexes(vtx v1alpha1.AbstractVertex) []int
}

type podTracker struct {
	pipeline        *v1alpha1.Pipeline
	log             *zap.SugaredLogger
	httpClient      monitorHttpClient
	activePods      *util.UniqueStringList
	refreshInterval time.Duration
}

func NewPodTracker(ctx context.Context, pl *v1alpha1.Pipeline) PodTracker {
	pt := &podTracker{
		pipeline: pl,
		log:      logging.FromContext(ctx).Named("RuntimepodTracker"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: 100 * time.Millisecond,
		},
		activePods:      util.NewUniqueStringList(),
		refreshInterval: 30 * time.Second, // Default refresh interval for updating the active pod set
	}
	return pt
}

func (pt *podTracker) Start(ctx context.Context) error {
	pt.log.Debugf("Starting tracking active pods for Pipeline %s...", pt.pipeline.Name)
	go pt.trackActivePods(ctx)
	return nil
}

func (pt *podTracker) trackActivePods(ctx context.Context) {
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
func (pt *podTracker) updateActivePods() {
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, v := range pt.pipeline.Spec.Vertices {
		for i := range int(v.Scale.GetMaxReplicas()) {
			wg.Add(1)
			go func(vertexName string, index int) {
				defer wg.Done()
				podName := fmt.Sprintf("%s-%s-%d", pt.pipeline.Name, vertexName, index)
				podKey := pt.getPodKey(index, vertexName)
				if pt.isActive(vertexName, podName) {
					mu.Lock()
					pt.activePods.PushBack(podKey)
					mu.Unlock()
				} else {
					mu.Lock()
					pt.activePods.Remove(podKey)
					mu.Unlock()
				}
			}(v.Name, i)
		}
	}
	wg.Wait()
	pt.log.Debugf("Finished updating the active pod set: %v", pt.activePods.ToString())
}

func (pt *podTracker) getPodKey(index int, vertexName string) string {
	// podKey is used as a unique identifier for the pod, which is a combination of pipeline name, vertex name and pod index.
	return strings.Join([]string{pt.pipeline.Name, vertexName, fmt.Sprintf("%d", index)}, podInfoSeparator)
}

func (pt *podTracker) isActive(vertexName, podName string) bool {
	// using the vertex headless service to check if a pod exists or not.
	// monitor sidecar container's endpoint is used to check if the pod is active or not.
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

// get active pod indexes to query the runtime errors endpoint.
func (pt *podTracker) GetActivePodIndexes(vtx v1alpha1.AbstractVertex) []int {
	var podIndexes []int
	values := pt.activePods.ToString()
	if values == "" {
		return podIndexes
	}

	podList := strings.Split(values, ",")
	for _, pod := range podList {
		// Extract the pod index from the pod name
		parts := strings.Split(pod, podInfoSeparator)
		if len(parts) != 3 {
			pt.log.Debugf("Unexpected pod name format: %s", pod)
			continue
		}
		if parts[1] != vtx.Name {
			pt.log.Debugf("Ignoring other vtx: %s", parts[1])
			continue
		}
		podIndex, err := strconv.Atoi(parts[2])
		if err != nil {
			pt.log.Errorf("Error converting pod index to int for pod %s: %v", pod, err)
			continue
		}
		podIndexes = append(podIndexes, podIndex)
	}
	return podIndexes
}
