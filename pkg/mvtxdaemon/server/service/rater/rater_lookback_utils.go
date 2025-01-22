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
	"fmt"
	"math"

	"github.com/prometheus/common/expfmt"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

// PodProcessingTime is a struct to maintain the required data
// to calculate the processing time for a batch by a pod
type PodProcessingTime struct {
	name                string
	processingTimeSum   float64
	processingTimeCount float64
}

func (p *PodProcessingTime) Name() string {
	return p.name
}

func (p *PodProcessingTime) processingTimeValues() (float64, float64) {
	return p.processingTimeSum, p.processingTimeCount
}

// updateDynamicLookbackSecs updates the default lookback period of a vertex based on the processing rate.
// It is intended to optimize the responsiveness of the system to changes based on its
// current load and performance characteristics.
func (r *Rater) updateDynamicLookbackSecs() {
	// calculate rates for each look back seconds
	vertexName := r.monoVertex.Name
	processingTimeSeconds, update := r.CalculateVertexProcessingTime(r.timestampedPodProcessingTime)
	if !update {
		return
	}
	r.log.Debugf("Calculated processingTimeSeconds for mvtx %s : %f ", vertexName, processingTimeSeconds)
	// if the current calculated processing time is greater than the lookback Seconds, update it
	currentVal := r.userSpecifiedLookBackSeconds.Load()
	// round up to the nearest minute, also ensure that while going up and down we have the consistent value for
	// a given processingTimeSeconds, then convert back to seconds
	roundedProcessingTime := 60 * int(math.Ceil(processingTimeSeconds/60))
	// step up case
	if roundedProcessingTime > int(currentVal) {
		r.userSpecifiedLookBackSeconds.Store(math.Min(MaxLookback.Seconds(), float64(roundedProcessingTime)))
		r.log.Infof("Lookback updated for mvtx %s, old %f new %d", vertexName, currentVal, roundedProcessingTime)
	} else {
		// step down case
		// We should not be setting values lower than the lookBackSeconds defined in the spec
		roundedProcessingTime = int(math.Max(float64(roundedProcessingTime), float64(r.monoVertex.Spec.Scale.GetLookbackSeconds())))
		if roundedProcessingTime != int(currentVal) {
			r.userSpecifiedLookBackSeconds.Store(float64(roundedProcessingTime))
			r.log.Infof("Lookback updated for mvtx %s, old %f new %d", vertexName, currentVal, roundedProcessingTime)
		}
	}
}

func (r *Rater) CalculateVertexProcessingTime(q *sharedqueue.OverflowQueue[*TimestampedProcessingTime]) (float64, bool) {
	counts := q.Items()
	currentLookbackSecs := r.userSpecifiedLookBackSeconds.Load()
	// If we do not have enough data points, lets send back the default from the vertex
	// or if we are gating at the max lookback
	if len(counts) <= 1 || (currentLookbackSecs >= MaxLookback.Seconds()) {
		return currentLookbackSecs, false
	}
	// Checking for 3 look back periods right now -> this will be gated to 30 min
	// as that is the max data that we store
	startIndex := findStartIndexPt(int64(currentLookbackSecs*3), counts)
	// we consider the last but one element as the end index because the last element might be incomplete
	// we can be sure that the last but one element in the queue is complete.
	endIndex := len(counts) - 2
	// If we do not have data from previous timeline, then return the current
	// lookback time itself
	if startIndex == indexNotFound {
		return currentLookbackSecs, false
	}

	// time diff in seconds.
	timeDiff := counts[endIndex].timestamp - counts[startIndex].timestamp
	if timeDiff == 0 {
		// if the time difference is 0, we return 0 to avoid division by 0
		// this should not happen in practice because we are using a 10s interval
		return currentLookbackSecs, false
	}

	cumulativeTime := make(map[string]float64)
	count := make(map[string]int)

	// Iterate over the range of items and aggregate it per pod level
	for i := startIndex; i <= endIndex; i++ {
		item := counts[i]
		if item == nil {
			continue
		}
		vals := item.PodProcessingTimeSnapshot()
		for pod, ptTime := range vals {
			cumulativeTime[pod] += ptTime
			count[pod]++
		}
	}

	// TODO(mvtx-adapt): Check with EWMA if that helps better
	maxAverage := 0.0
	// Calculate averages and find the maximum between the pods
	for pod, totalTime := range cumulativeTime {
		if totalCnt := count[pod]; totalCnt > 0 {
			average := totalTime / float64(totalCnt)
			if average > maxAverage {
				maxAverage = average
			}
		}

	}
	// Return the maximum average processing time
	return maxAverage, true
}

// getPodProcessingTime is a utility function to get the metrics from the pod
func (r *Rater) getPodProcessingTime(podName string) *PodProcessingTime {
	processingTimeCountMetric := "monovtx_processing_time"
	headlessServiceName := r.monoVertex.GetHeadlessServiceName()
	// scrape the read total metric from pod metric port
	// example for 0th pod: https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless.default.svc:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/metrics", podName, headlessServiceName, r.monoVertex.Namespace, v1alpha1.MonoVertexMetricsPort)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		r.log.Warnf("[Pod name %s]: failed reading the metrics endpoint, the pod might have been scaled down: %v", podName, err.Error())
		return nil
	}
	defer resp.Body.Close()

	textParser := expfmt.TextParser{}
	result, err := textParser.TextToMetricFamilies(resp.Body)
	if err != nil {
		r.log.Errorf("[Pod name %s]:  failed parsing to prometheus metric families, %v", podName, err.Error())
		return nil
	}

	var podSum, podCount float64
	if value, ok := result[processingTimeCountMetric]; ok && value != nil && len(value.GetMetric()) > 0 {
		metricsList := value.GetMetric()
		// Each pod should be emitting only one metric with this name, so we should be able to take the first value
		// from the results safely.
		// https://github.com/prometheus/client_rust/issues/194
		ele := metricsList[0]
		podCount = float64(ele.Histogram.GetSampleCount())
		podSum = ele.Histogram.GetSampleSum()
	} else {
		r.log.Debugf("[Pod name %s]: Metric %q is unavailable, the pod might haven't started processing data", podName, processingTimeCountMetric)
		return nil
	}
	return &PodProcessingTime{
		name:                podName,
		processingTimeSum:   podSum,
		processingTimeCount: podCount,
	}
}
