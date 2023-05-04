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

	"github.com/prometheus/common/expfmt"

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
					fmt.Printf("Keran is testing, the podTotalCounts is %v", podTotalCounts)
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
	var err error
	var result = make(map[string]float64)

	index := 0
	for {
		podName := fmt.Sprintf("%s-%s-%d", rc.pipeline.Name, vertex.Name, index)
		if rc.podExists(vertex.Name, podName) {
			fmt.Printf("Keran is testing, found pod %s\n", podName)
			result[podName], err = rc.getTotalCount(ctx, vertex, podName)
			// TODO - maybe just COUNT_NOT_AVAILABLE?
			if err != nil {
				fmt.Printf("Keran is testing, failed to get total count for pod %s: %v\n", podName, err.Error())
				return nil, fmt.Errorf("failed to get total count for pod %s: %v", podName, err.Error())
			}
		} else {
			fmt.Printf("Keran is testing, didn't find pod %s\n", podName)
			// TODO - only break when 3 consecutive pods are not found
			break
		}
		index++
	}
	return result, nil
}

func (rc *RateCalculator) getTotalCount(_ context.Context, vertex *v1alpha1.AbstractVertex, podName string) (float64, error) {
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, rc.pipeline.Name+"-"+vertex.Name+"-headless", rc.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if res, err := rc.httpClient.Get(url); err != nil {
		// TODO - replace 0 with COUNT_NOT_AVAILABLE
		fmt.Printf("Keran is testing, failed reading the metrics endpoint, %v", err.Error())
		return 0, fmt.Errorf("failed reading the metrics endpoint, %v", err.Error())
	} else {
		// parse the metrics
		textParser := expfmt.TextParser{}
		result, err := textParser.TextToMetricFamilies(res.Body)
		if err != nil {
			fmt.Printf("Keran is testing, failed parsing to prometheus metric families, %v", err.Error())
			return 0, fmt.Errorf("failed parsing to prometheus metric families, %v", err.Error())
		}

		// TODO - reducer is using a different name, need to differentiate between the two
		if value, ok := result["forwarder_read_total"]; ok && value != nil && len(value.GetMetric()) > 0 {
			metricsList := value.GetMetric()
			fmt.Printf("Keran is testing, got the count for pod %s is %v\n", podName, metricsList[0].Counter.GetValue())
			return metricsList[0].Counter.GetValue(), nil
		} else {
			return 0, fmt.Errorf("forwarder_read_total metric not found")
		}
	}
}

func (rc *RateCalculator) podExists(vertexName, podName string) bool {
	// we can query the metrics endpoint of the (i)th pod to obtain this value.
	// example for 0th pod : https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc.cluster.local:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, rc.pipeline.Name+"-"+vertexName+"-headless", rc.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	// TODO - Head is enough?
	if _, err := rc.httpClient.Get(url); err != nil {
		fmt.Printf("Keran is testing, didn't find pod %s, err is %v", podName, err.Error())
		return false
	}
	fmt.Printf("Keran is testing, found pod %s", podName)
	return true
}
