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
	"os"
	"time"

	"github.com/prometheus/common/expfmt"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

// fixedLookbackSeconds Always maintain rate metrics for the following lookback seconds (1m, 5m, 15m)
var fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}

// RateCalculator is a struct that calculates the processing rate of a vertex.
type RateCalculator struct {
	// TODO - do we need to pass in the entire pipeline or just the vertex?
	pipeline   *v1alpha1.Pipeline
	vertex     *v1alpha1.AbstractVertex
	httpClient *http.Client
	kubeClient kubernetes.Interface

	timestampedTotalCounts *sharedqueue.OverflowQueue[TimestampedCount]
	lastSawPodCounts       map[string]float64
	processingRates        map[string]float64
	refreshInterval        time.Duration

	userSpecifiedLookback int64
}

// NewRateCalculator creates a new rate calculator.
func NewRateCalculator(p *v1alpha1.Pipeline, v *v1alpha1.AbstractVertex) *RateCalculator {
	rc := RateCalculator{
		pipeline: p,
		vertex:   v,
		// TODO - should we create a new client for each vertex or share one client for all vertices?
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 3,
		},
		// maintain the total counts of the last 30 minutes since we support 1m, 5m, 15m lookback seconds.
		timestampedTotalCounts: sharedqueue.New[TimestampedCount](1800),
		lastSawPodCounts:       make(map[string]float64),
		processingRates:        make(map[string]float64),
		refreshInterval:        5 * time.Second, // Default refresh interval

		userSpecifiedLookback: int64(v.Scale.GetLookbackSeconds()),
	}

	var err error
	kubeConfig := os.Getenv("KUBECONFIG")
	if kubeConfig == "" {
		home, _ := os.UserHomeDir()
		kubeConfig = home + "/.kube/config"
		if _, err := os.Stat(kubeConfig); err != nil && os.IsNotExist(err) {
			kubeConfig = ""
		}
	}
	var restConfig *rest.Config
	if kubeConfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
	} else {
		restConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		// TODO - logger
		fmt.Println("Failed to create rest config")
		return nil
	}
	rc.kubeClient, err = kubernetes.NewForConfig(restConfig)

	if err != nil {
		// TODO - logger
		fmt.Println("Failed to create kube client")
		return nil
	}

	return &rc
}

// Start TODO - how to stop and how to handle errors? seems the caller will do ctx done as signal to stop.
// Start starts the rate calculator that periodically fetches the total counts, calculates and updates the rates.
func (rc *RateCalculator) Start(ctx context.Context) error {
	log := logging.FromContext(ctx).Named("RateCalculator")
	log.Infof("Starting rate calculator for vertex %s...", rc.vertex.Name)

	lookbackSecondsMap := map[string]int64{"default": rc.userSpecifiedLookback}
	for k, v := range fixedLookbackSeconds {
		lookbackSecondsMap[k] = v
	}

	log.Infof("Keran is testing, the refresh interval is %v", rc.refreshInterval)

	go func() {
		ticker := time.NewTicker(rc.refreshInterval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// TODO - fetch total counts from the vertex and update the timestamped total counts queue
				podTotalCounts, err := rc.findCurrentTotalCounts(ctx, rc.vertex)
				if err != nil {
					println("Keran is testing, the err is ", err.Error())
				} else {
					println("Keran is testing, the podTotalCounts is ", podTotalCounts)
				}

				// update counts trackers
				UpdateCountTrackers(rc.timestampedTotalCounts, rc.lastSawPodCounts, podTotalCounts)

				// calculate rates for each lookback seconds
				for n, i := range lookbackSecondsMap {
					r := CalculateRate(rc.timestampedTotalCounts, i)
					log.Infof("Updating rate for vertex %s, lookback period is %s, rate is %v", rc.vertex.Name, n, r)
					rc.processingRates[n] = r
				}
			}
		}
	}()

	return nil
}

func (rc *RateCalculator) GetRates() map[string]float64 {
	return rc.processingRates
}

// TODO - rethink about error handling
func (rc *RateCalculator) findCurrentTotalCounts(ctx context.Context, vertex *v1alpha1.AbstractVertex) (map[string]float64, error) {
	selector := v1alpha1.KeyPipelineName + "=" + rc.pipeline.Name + "," + v1alpha1.KeyVertexName + "=" + vertex.Name
	var pods *v1.PodList
	var err error
	if pods, err = rc.kubeClient.CoreV1().Pods(rc.pipeline.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector}); err != nil {
		return nil, fmt.Errorf("failed to list pods: %v", err.Error())
	}
	var result = make(map[string]float64)
	for _, v := range pods.Items {
		count, err := rc.getTotalCount(ctx, vertex, v)
		if err != nil {
			return nil, fmt.Errorf("failed to get total count for pod %s: %v", v.Name, err.Error())
		} else {
			result[v.Name] = count
		}
	}
	return result, nil
}

func (rc *RateCalculator) getTotalCount(_ context.Context, vertex *v1alpha1.AbstractVertex, pod v1.Pod) (float64, error) {
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", pod.Name, vertex.Name+"-headless", rc.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if res, err := rc.httpClient.Get(url); err != nil {
		// TODO - replace 0 with COUNT_NOT_AVAILABLE
		return 0, fmt.Errorf("failed reading the metrics endpoint, %v", err.Error())
	} else {
		// parse the metrics
		textParser := expfmt.TextParser{}
		result, err := textParser.TextToMetricFamilies(res.Body)
		if err != nil {
			return 0, fmt.Errorf("failed parsing to prometheus metric families, %v", err.Error())
		}

		if value, ok := result["forwarder_read_total"]; ok && value != nil && len(value.GetMetric()) > 0 {
			metricsList := value.GetMetric()
			return metricsList[0].Counter.GetValue(), nil
		} else {
			return 0, fmt.Errorf("forwarder_read_total metric not found")
		}
	}
}

/*
func (rc *RateCalculator) podExists(podName string) bool {

	return false
}

*/
