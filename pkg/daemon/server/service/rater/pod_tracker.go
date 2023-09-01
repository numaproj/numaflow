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

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

// PodInfoSeparator is used as a separator such that the worker can split the pod key
// to get the pipeline name, vertex name, pod index and vertex type.
// "*" is chosen because it is not allowed in all the above fields.
const PodInfoSeparator = "*"

// PodTracker maintains a set of active pods for a pipeline
// It periodically sends http requests to pods to check if they are still active
type PodTracker struct {
	pipeline        *v1alpha1.Pipeline
	log             *zap.SugaredLogger
	httpClient      metricsHttpClient
	activePods      *UniqueStringList
	refreshInterval time.Duration
}

func NewPodTracker(ctx context.Context, p *v1alpha1.Pipeline, opts ...PodTrackerOption) *PodTracker {
	pt := PodTracker{
		pipeline: p,
		log:      logging.FromContext(ctx).Named("PodTracker"),
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second,
		},
		activePods:      NewUniqueStringList(),
		refreshInterval: 30 * time.Second, // Default refresh interval for updating active pod set
	}

	for _, opt := range opts {
		if opt != nil {
			opt(&pt)
		}
	}
	return &pt
}

type PodTrackerOption func(*PodTracker)

// WithRefreshInterval sets how often to refresh the rate metrics.
func WithRefreshInterval(d time.Duration) PodTrackerOption {
	return func(r *PodTracker) {
		r.refreshInterval = d
	}
}

func (pt *PodTracker) Start(ctx context.Context) error {
	pt.log.Debugf("Starting tracking active pods for pipeline %s...", pt.pipeline.Name)
	vNames := ""
	for _, v := range pt.pipeline.Spec.Vertices {
		vNames += v.Name + ","
	}
	// Ok, at this moment, the out vertex is present.
	pt.log.Debugf("The pipeline contains the following vertices %s", vNames)
	go func() {
		ticker := time.NewTicker(pt.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				pt.log.Infof("Context is cancelled. Stopping tracking active pods for pipeline %s...", pt.pipeline.Name)
				return
			case <-ticker.C:
				for _, v := range pt.pipeline.Spec.Vertices {
					pt.log.Debugf("Updating the active pod set for vertex %s, vertex repli", v.Name)
					var vType string
					if v.IsReduceUDF() {
						vType = "reduce"
					} else if v.IsASource() {
						vType = "source"
					} else {
						vType = "other"
					}
					for i := 0; i < int(v.Scale.GetMaxReplicas()); i++ {
						podName := fmt.Sprintf("%s-%s-%d", pt.pipeline.Name, v.Name, i)
						podKey := pt.getPodKey(i, v.Name, vType)
						if pt.isActive(v.Name, podName) {
							pt.activePods.PushBack(podKey)
						} else {
							// if the pod is not active, remove it from the active pod list
							pt.activePods.Remove(podKey)
						}
					}
				}
				pt.log.Debugf("Finished updating the active pod set: %v", pt.activePods.ToString())
			}
		}
	}()
	return nil
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

// IsActive returns true if the pod is active, false otherwise.
func (pt *PodTracker) IsActive(podKey string) bool {
	return pt.activePods.Contains(podKey)
}

// GetActivePodsCount returns the number of active pods.
func (pt *PodTracker) GetActivePodsCount() int {
	return pt.activePods.Length()
}

func (pt *PodTracker) getPodKey(index int, vertexName string, vertexType string) string {
	// podKey is used as a unique identifier for the pod, it is used by worker to determine the count of processed messages of the pod.
	return strings.Join([]string{pt.pipeline.Name, vertexName, fmt.Sprintf("%d", index), vertexType}, PodInfoSeparator)
}

func (pt *PodTracker) isActive(vertexName, podName string) bool {
	// using the vertex headless service to check if a pod exists or not.
	// example for 0th pod: https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/metrics", podName, pt.pipeline.Name+"-"+vertexName+"-headless", pt.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	resp, err := pt.httpClient.Head(url)
	if err != nil {
		// during performance test (100 pods per vertex), we never saw a false negative, meaning every time isActive returns false,
		// it truly means the pod doesn't exist.
		// in reality, we can imagine that a pod can be active but the Head request times out for some reason and returns an incorrect false,
		// if we ever observe such case, we can think about adding retry here.

		// Even though I can see 4 pods for out sink, the pod tracker gives
		// "Sending HEAD request to pod raptor-pipeline-out-0 is unsuccessful"
		pt.log.Debugf("Sending HEAD request to pod %s is unsuccessful: %v, treating the pod as inactive", podName, err)
		return false
	}
	_ = resp.Body.Close()
	return true
}
