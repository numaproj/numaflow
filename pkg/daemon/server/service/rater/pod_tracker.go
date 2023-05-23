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
	"time"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

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
	pt.log.Infof("Starting tracking active pods for pipeline %s...", pt.pipeline.Name)
	go func() {
		ticker := time.NewTicker(pt.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, v := range pt.pipeline.Spec.Vertices {
					var limit int
					if max := v.Scale.Max; max != nil {
						limit = int(*max)
					} else {
						limit = v1alpha1.MaxReplicasLimit
					}
					var vType string
					if v.IsReduceUDF() {
						vType = "reduce"
					} else {
						vType = "non_reduce"
					}
					for i := 0; i < limit; i++ {
						podName := fmt.Sprintf("%s-%s-%d", pt.pipeline.Name, v.Name, i)
						// podKey is used as a unique identifier for the pod, it is used by worker to determine the count of processed messages of the pod.
						// "*" is used as a separator such that the worker can split the key to get the pipeline name, vertex name, pod index and vertex type.
						podKey := fmt.Sprintf("%s*%s*%d*%s", pt.pipeline.Name, v.Name, i, vType)
						if pt.isActive(v.Name, podName) {
							pt.activePods.PushBack(podKey)
						} else {
							pt.activePods.Remove(podKey)
							// we assume all the pods are ordered with continuous indices, hence as we keep increasing the index, if we don't find one, we can stop looking.
							// the assumption holds because when we scale down, we always scale down from the last pod.
							// there can be a case when a pod in the middle crashes, causing us missing counting the following pods.
							// such case is rare and if it happens, it can lead to lower rate then the real one. It is acceptable because it will recover when the crashed pod is restarted.
							break
						}
					}
				}
				pt.log.Debugf("Finished updating the active pod set: %v", pt.activePods.ToString())
			}
		}
	}()
	return nil
}

func (pt *PodTracker) GetActivePods() *UniqueStringList {
	return pt.activePods
}

func (pt *PodTracker) isActive(vertexName, podName string) bool {
	// using the vertex headless service to check if a pod exists or not.
	// example for 0th pod : https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc.cluster.local:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, pt.pipeline.Name+"-"+vertexName+"-headless", pt.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if _, err := pt.httpClient.Head(url); err != nil {
		return false
	}
	return true
}
