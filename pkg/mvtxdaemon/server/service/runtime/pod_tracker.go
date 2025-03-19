package runtime

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"go.uber.org/zap"
)

type PodTracker struct {
	monoVertex      *v1alpha1.MonoVertex
	log             *zap.SugaredLogger
	httpClient      monitorHttpClient
	activePods      *util.UniqueStringList
	refreshInterval time.Duration
}

func NewPodTracker(ctx context.Context, mv *v1alpha1.MonoVertex) *PodTracker {
	pt := &PodTracker{
		monoVertex: mv,
		log:        logging.FromContext(ctx).Named("RuntimePodTracker"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second,
		},
		activePods:      util.NewUniqueStringList(),
		refreshInterval: 30 * time.Second,
	}
	return pt
}

func (pt *PodTracker) Start(ctx context.Context) error {
	pt.log.Debugf("Starting tracking active pods for MonoVertex %s...", pt.monoVertex.Name)
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
			pt.log.Infof("Context is cancelled. Stopping tracking active pods for MonoVertex %s...", pt.monoVertex.Name)
			return
		case <-ticker.C:
			pt.updateActivePods()
		}
	}
}

// updateActivePods checks the status of all pods and updates the activePods set accordingly.
func (pt *PodTracker) updateActivePods() {
	var wg sync.WaitGroup
	for i := range int(pt.monoVertex.Spec.Scale.GetMaxReplicas()) {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			podName := fmt.Sprintf("%s-mv-%d", pt.monoVertex.Name, index)
			podKey := fmt.Sprintf("%d", index)
			if pt.isActive(podName) {
				pt.activePods.PushBack(podKey)
			} else {
				pt.activePods.Remove(podKey)
			}
		}(i)
	}
	wg.Wait()
	pt.log.Debugf("Finished updating the active pod set: %v", pt.activePods.ToString())
}

func (pt *PodTracker) isActive(podName string) bool {
	headlessSvc := pt.monoVertex.GetHeadlessServiceName()
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

// returns the number of active pods.
func (pt *PodTracker) GetActivePodsCount() int {
	return pt.activePods.Length()
}
