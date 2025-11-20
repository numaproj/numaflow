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

package rater

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// podInfoSeparator is used as a separator to split the pod key
// to get the pipeline name, vertex name, and pod index.
// "*" is chosen because it is not allowed in all the above fields.
const podInfoSeparator = "*"

// PodTracker maintains a set of active pods for a MonoVertex
// It periodically sends http requests to pods to check if they are still active
type PodTracker struct {
	monoVertex      *v1alpha1.MonoVertex
	log             *zap.SugaredLogger
	httpClient      metricsHttpClient
	activePods      *util.UniqueStringList
	refreshInterval time.Duration
}
type PodTrackerOption func(*PodTracker)

func NewPodTracker(ctx context.Context, mv *v1alpha1.MonoVertex, opts ...PodTrackerOption) *PodTracker {
	pt := &PodTracker{
		monoVertex: mv,
		log:        logging.FromContext(ctx).Named("PodTracker"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second,
		},
		activePods:      util.NewUniqueStringList(),
		refreshInterval: 30 * time.Second, // Default refresh interval for updating the active pod set
	}

	for _, opt := range opts {
		if opt != nil {
			opt(pt)
		}
	}
	return pt
}

// WithRefreshInterval sets how often to refresh the rate metrics.
func WithRefreshInterval(d time.Duration) PodTrackerOption {
	return func(r *PodTracker) {
		r.refreshInterval = d
	}
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
			podKey := pt.getPodKey(index)
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

func (pt *PodTracker) getPodKey(index int) string {
	// podKey is used as a unique identifier for the pod, it is used by worker to determine the count of processed messages of the pod.
	// we use the monoVertex name and the pod index to create a unique identifier.
	// For example, if the monoVertex name is "simple-mono-vertex" and the pod index is 0, the podKey will be "simple-mono-vertex*0".
	// This way, we can easily identify the pod based on its key.
	return strings.Join([]string{pt.monoVertex.Name, fmt.Sprintf("%d", index)}, podInfoSeparator)
}

// IsActive returns true if the pod is active, false otherwise.
func (pt *PodTracker) IsActive(podKey string) bool {
	return pt.activePods.Contains(podKey)
}

func (pt *PodTracker) isActive(podName string) bool {
	headlessSvc := pt.monoVertex.GetHeadlessServiceName()
	// using the MonoVertex headless service to check if a pod exists or not.
	// example for 0th pod: https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless.default.svc:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/metrics", podName, headlessSvc, pt.monoVertex.Namespace, v1alpha1.MonoVertexMetricsPort)
	resp, err := pt.httpClient.Head(url)
	if err != nil {
		pt.log.Debugf("Sending HEAD request to pod %s is unsuccessful: %v, treating the pod as inactive", podName, err)
		return false
	}
	pt.log.Debugf("Sending HEAD request to pod %s is successful, treating the pod as active", podName)
	_ = resp.Body.Close()
	return true
}

// GetActivePodsCount returns the number of active pods.
func (pt *PodTracker) GetActivePodsCount() int {
	return pt.activePods.Length()
}

// PodInfo represents the information of a pod that is used for tracking the processing rate
type PodInfo struct {
	monoVertexName string
	replica        int
	podName        string
}

func (pt *PodTracker) GetPodInfo(key string) (*PodInfo, error) {
	pi := strings.Split(key, podInfoSeparator)
	if len(pi) != 2 {
		return nil, fmt.Errorf("invalid key %q", key)
	}
	replica, err := strconv.Atoi(pi[1])
	if err != nil {
		return nil, fmt.Errorf("invalid replica in key %q", key)
	}
	return &PodInfo{
		monoVertexName: pi[0],
		replica:        replica,
		podName:        strings.Join([]string{pi[0], "mv", pi[1]}, "-"),
	}, nil
}

// LeastRecentlyUsed returns the least recently used pod from the active pod list.
// if there are no active pods, it returns an empty string.
func (pt *PodTracker) LeastRecentlyUsed() string {
	if e := pt.activePods.Front(); e != "" {
		pt.activePods.MoveToBack(e)
		return e
	}
	return ""
}
