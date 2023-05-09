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
	"sync"
	"time"

	"github.com/prometheus/common/expfmt"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

// metricsHttpClient interface for the GET/HEAD call to metrics endpoint.
// Had to add this an interface for testing
type metricsHttpClient interface {
	Get(url string) (*http.Response, error)
	Head(url string) (*http.Response, error)
}

// CountNotAvailable indicates that the rate calculator was not able to retrieve the count
const CountNotAvailable = float64(math.MinInt)

// fixedLookbackSeconds Always maintain rate metrics for the following lookback seconds (1m, 5m, 15m)
var fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}

// RateCalculator is a struct that calculates the processing rate of a vertex.
type RateCalculator struct {
	pipeline   *v1alpha1.Pipeline
	vertex     *v1alpha1.AbstractVertex
	httpClient metricsHttpClient

	timestampedTotalCounts *sharedqueue.OverflowQueue[TimestampedCount]
	lastSawPodCounts       map[string]float64
	processingRates        map[string]float64
	processingRatesLock    *sync.RWMutex
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
		processingRatesLock:    new(sync.RWMutex),
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
		ticker := time.NewTicker(rc.refreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rc.processingRatesLock.Lock()
				podTotalCounts := rc.findCurrentTotalCounts(ctx, rc.vertex)
				// update counts trackers
				UpdateCountTrackers(rc.timestampedTotalCounts, rc.lastSawPodCounts, podTotalCounts)
				// calculate rates for each lookback seconds
				for n, i := range lookbackSecondsMap {
					r := CalculateRate(rc.timestampedTotalCounts, i)
					rc.processingRates[n] = r
				}
				rc.processingRatesLock.Unlock()
			}
		}
	}()
	return nil
}

// GetRates returns the processing rates of the vertex in the format of lookback second to rate mappings
func (rc *RateCalculator) GetRates() map[string]float64 {
	rc.processingRatesLock.RLock()
	defer rc.processingRatesLock.RUnlock()
	return rc.processingRates
}

// findCurrentTotalCounts build a total count map with key being the pod name and value being the current total number of messages read by the pod
func (rc *RateCalculator) findCurrentTotalCounts(ctx context.Context, vertex *v1alpha1.AbstractVertex) map[string]float64 {
	var result = make(map[string]float64)
	resultChan := make(chan struct {
		podName string
		count   float64
	})
	var wg sync.WaitGroup

	// Collect the results
	go func() {
		for podResult := range resultChan {
			result[podResult.podName] = podResult.count
		}
	}()

	index := 0
	for {
		podName := fmt.Sprintf("%s-%s-%d", rc.pipeline.Name, vertex.Name, index)
		exists := rc.podExists(vertex.Name, podName)
		if exists {
			// run getTotalCount calls in parallel
			wg.Add(1)
			go func(podName string) {
				defer wg.Done()
				count := rc.getTotalCount(ctx, vertex, podName)
				resultChan <- struct {
					podName string
					count   float64
				}{podName, count}
			}(podName)
		} else {
			// we assume all the pods are in order, hence if we don't find one, we can stop looking
			// this is because when we scale down, we always scale down from the last pod
			// there can be a case when a pod in the middle crashes, hence we miss counting the following pods
			// but this is rare, if it happens, we end up getting a rate that's lower than the real one and wait for the next refresh to recover
			break
		}
		index++
	}
	wg.Wait()
	close(resultChan)
	return result
}

// getTotalCount returns the total number of messages read by the pod
func (rc *RateCalculator) getTotalCount(ctx context.Context, vertex *v1alpha1.AbstractVertex, podName string) float64 {
	log := logging.FromContext(ctx).Named("RateCalculator")
	// scrape the read total metric from pod metric port
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, rc.pipeline.Name+"-"+vertex.Name+"-headless", rc.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if res, err := rc.httpClient.Get(url); err != nil {
		log.Errorf("failed reading the metrics endpoint, %v", err.Error())
		return CountNotAvailable
	} else {
		textParser := expfmt.TextParser{}
		result, err := textParser.TextToMetricFamilies(res.Body)
		if err != nil {
			log.Errorf("failed parsing to prometheus metric families, %v", err.Error())
			return CountNotAvailable
		}

		var readTotalMetricName string
		if vertex.IsReduceUDF() {
			readTotalMetricName = "reduce_isb_reader_read_total"
		} else {
			readTotalMetricName = "forwarder_read_total"
		}

		if value, ok := result[readTotalMetricName]; ok && value != nil && len(value.GetMetric()) > 0 {
			metricsList := value.GetMetric()
			return metricsList[0].Counter.GetValue()
		} else {
			return CountNotAvailable
		}
	}
}

func (rc *RateCalculator) podExists(vertexName, podName string) bool {
	// using the vertex headless service to check if a pod exists or not.
	// example for 0th pod : https://simple-pipeline-in-0.simple-pipeline-in-headless.default.svc.cluster.local:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc.cluster.local:%v/metrics", podName, rc.pipeline.Name+"-"+vertexName+"-headless", rc.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	if _, err := rc.httpClient.Head(url); err != nil {
		return false
	}
	return true
}
