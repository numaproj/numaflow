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
// to get the MonoVertex name and pod index.
// "*" is chosen because it is not allowed in the above fields.
const podInfoSeparator = "*"

type PodTracker interface {
	Start(ctx context.Context) error
	GetActivePodIndexes() []int
}
type podTracker struct {
	monoVertex      *v1alpha1.MonoVertex
	log             *zap.SugaredLogger
	httpClient      monitorHttpClient
	activePods      *util.UniqueStringList
	refreshInterval time.Duration
}

func NewPodTracker(ctx context.Context, mv *v1alpha1.MonoVertex) PodTracker {
	pt := &podTracker{
		monoVertex: mv,
		log:        logging.FromContext(ctx).Named("RuntimePodTracker"),
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
	pt.log.Debugf("Starting tracking active pods for MonoVertex %s...", pt.monoVertex.Name)
	go pt.trackActivePods(ctx)
	return nil
}

func (pt *podTracker) trackActivePods(ctx context.Context) {
	ticker := time.NewTicker(pt.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			pt.log.Infof("Context is cancelled. Stopping tracking active pods for MonoVertex %s...", pt.monoVertex.Name)
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

	for i := range int(pt.monoVertex.Spec.Scale.GetMaxReplicas()) {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			podName := fmt.Sprintf("%s-mv-%d", pt.monoVertex.Name, index)
			podKey := pt.getPodKey(index)
			if pt.isActive(podName) {
				mu.Lock()
				pt.activePods.PushBack(podKey)
				mu.Unlock()
			} else {
				mu.Lock()
				pt.activePods.Remove(podKey)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	pt.log.Debugf("Finished updating the active pod set: %v", pt.activePods.ToString())
}

func (pt *podTracker) getPodKey(index int) string {
	// podKey is used as a unique identifier for the pod, it is used by worker to determine the count of processed messages of the pod.
	// we use the monoVertex name and the pod index to create a unique identifier.
	// For example, if the monoVertex name is "simple-mono-vertex" and the pod index is 0, the podKey will be "simple-mono-vertex*0".
	// This way, we can easily identify the pod based on its key.
	return strings.Join([]string{pt.monoVertex.Name, fmt.Sprintf("%d", index)}, podInfoSeparator)
}

func (pt *podTracker) isActive(podName string) bool {
	headlessSvc := pt.monoVertex.GetHeadlessServiceName()
	// using the MonoVertex headless service to check if a pod exists or not.
	// monitor sidecar container's endpoint is used to check if the pod is active or not.
	// example for 0th pod: https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless.default.svc:2470/runtime/errors
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/runtime/errors", podName, headlessSvc, pt.monoVertex.Namespace, v1alpha1.MonoVertexMonitorPort)
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
func (pt *podTracker) GetActivePodIndexes() []int {
	var podIndexes []int
	values := pt.activePods.ToString()
	if values == "" {
		return podIndexes
	}

	podList := strings.Split(values, ",")
	for _, pod := range podList {
		// Extract the pod index from the pod name
		parts := strings.Split(pod, podInfoSeparator)
		if len(parts) != 2 {
			pt.log.Debugf("Unexpected pod name format: %s", pod)
			continue
		}
		podIndex, err := strconv.Atoi(parts[1])
		if err != nil {
			pt.log.Errorf("Error converting pod index to int for pod %s: %v", pod, err)
			continue
		}
		podIndexes = append(podIndexes, podIndex)
	}
	return podIndexes
}
