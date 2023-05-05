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
	"math"
	"net/http"
	"time"

	"github.com/prometheus/common/expfmt"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const (
	// CountNotAvailable indicates that the rate calculator was not able to retrieve the count
	CountNotAvailable = float64(math.MinInt)
	// RateNotAvailable indicates that the rate calculator was not able to calculate the rate.
	RateNotAvailable = float64(math.MinInt)
)

// fixedLookbackSeconds Always maintain rate metrics for the following lookback seconds (1m, 5m, 15m)
var fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}

// RateCalculator is a struct that calculates the processing rate of a vertex.
type RateCalculator struct {
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

// Start starts the rate calculator that periodically fetches the total counts, calculates and updates the rates.
func (rc *RateCalculator) Start(ctx context.Context) error {
	log := logging.FromContext(ctx).Named("RateCalculator")
	log.Infof("Starting rate calculator for vertex %s...", rc.vertex.Name)

	lookbackSecondsMap := map[string]int64{"default": rc.userSpecifiedLookback}
	for k, v := range fixedLookbackSeconds {
		lookbackSecondsMap[k] = v
	}
	go func() {
		log.Infof("Running rate calculator for vertex %s...", rc.vertex.Name)
		ticker := time.NewTicker(rc.refreshInterval)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
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
// Printing error can be useful for debugging, but should we just assign CountNotAvailable when error happens?
func (rc *RateCalculator) findCurrentTotalCounts(ctx context.Context, vertex *v1alpha1.AbstractVertex) (map[string]float64, error) {
	var err error
	var result = make(map[string]float64)

	index := 0
	for {
		podName := fmt.Sprintf("%s-%s-%d", rc.pipeline.Name, vertex.Name, index)
		if rc.podExists(vertex.Name, podName) {
			fmt.Printf("Keran is testing, found pod %s\n", podName)
			result[podName], err = rc.getTotalCount(ctx, vertex, podName)
			if err != nil {
				fmt.Printf("Keran is testing, failed to get total count for pod %s: %v\n", podName, err.Error())
			}
		} else {
			// we assume all the pods are in order, so if we don't find one, we can break
			// this is because when we scale down, we always scale down from the last pod
			// there can be a case when a pod in the middle crashes, hence we miss counting the following pods
			// but this is rare, and if it happens, we will just have a slightly lower rate and wait for the next refresh to recover
			fmt.Printf("Keran is testing, didn't find pod %s\n", podName)
			break
		}
		index++
	}
	return result, nil
}

func (rc *RateCalculator) getTotalCount(_ context.Context, vertex *v1alpha1.AbstractVertex, podName string) (float64, error) {
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, rc.pipeline.Name+"-"+vertex.Name+"-headless", rc.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if res, err := rc.httpClient.Get(url); err != nil {
		return CountNotAvailable, fmt.Errorf("failed reading the metrics endpoint, %v", err.Error())
	} else {
		textParser := expfmt.TextParser{}
		result, err := textParser.TextToMetricFamilies(res.Body)
		if err != nil {
			return CountNotAvailable, fmt.Errorf("failed parsing to prometheus metric families, %v", err.Error())
		}

		var readTotalMetricName string
		if vertex.IsReduceUDF() {
			readTotalMetricName = "reduce_isb_reader_read_total"
		} else {
			readTotalMetricName = "forwarder_read_total"
		}

		if value, ok := result[readTotalMetricName]; ok && value != nil && len(value.GetMetric()) > 0 {
			metricsList := value.GetMetric()
			fmt.Printf("Keran is testing, got the count for pod %s is %v\n", podName, metricsList[0].Counter.GetValue())
			return metricsList[0].Counter.GetValue(), nil
		} else {
			return CountNotAvailable, fmt.Errorf("read_total metric not found")
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
