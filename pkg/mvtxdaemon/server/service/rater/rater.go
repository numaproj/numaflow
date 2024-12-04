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
	"crypto/tls"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/common/expfmt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const CountWindow = time.Second * 10
const monoVtxReadMetricName = "monovtx_read_total"

// MonoVtxRatable is the interface for the Rater struct.
type MonoVtxRatable interface {
	Start(ctx context.Context) error
	GetRates() map[string]*wrapperspb.DoubleValue
}

var _ MonoVtxRatable = (*Rater)(nil)

// metricsHttpClient interface for the GET/HEAD call to metrics endpoint.
// Had to add this an interface for testing
type metricsHttpClient interface {
	Get(url string) (*http.Response, error)
	Head(url string) (*http.Response, error)
}

// fixedLookbackSeconds always maintain rate metrics for the following lookback seconds (1m, 5m, 15m)
var fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}

// Rater is a struct that maintains information about the processing rate of the MonoVertex.
// It monitors the number of processed messages for each pod in a MonoVertex and calculates the rate.
type Rater struct {
	monoVertex *v1alpha1.MonoVertex
	httpClient metricsHttpClient
	log        *zap.SugaredLogger
	// podTracker keeps track of active pods and their counts
	podTracker *PodTracker
	// timestampedPodCounts is a queue of timestamped counts for the MonoVertex
	timestampedPodCounts *sharedqueue.OverflowQueue[*TimestampedCounts]
	// timestampedPodProcessingTime is a map between vertex name and a queue of timestamped processing times for that vertex
	timestampedPodProcessingTime *sharedqueue.OverflowQueue[*TimestampedProcessingTime]
	// userSpecifiedLookBackSeconds is a map between vertex name and the user-specified lookback seconds for that vertex
	userSpecifiedLookBackSeconds *atomic.Float64
	options                      *options
}

// PodReadCount is a struct to maintain count of messages read by a pod of MonoVertex
type PodReadCount struct {
	// pod name of the pod
	name string
	// represents the count of messages read by the pod
	readCount float64
}

// Name returns the pod name
func (p *PodReadCount) Name() string {
	return p.name
}

// ReadCount returns the value of the messages read by the Pod
func (p *PodReadCount) ReadCount() float64 {
	return p.readCount
}

// PodProcessingTime is a struct to maintain processing time for a batch by a pod
type PodProcessingTime struct {
	// pod name
	name                string
	processingTimeSum   float64
	processingTimeCount float64
}

func (p *PodProcessingTime) Name() string {
	return p.name
}

func (p *PodProcessingTime) processingTime() (float64, float64) {
	return p.processingTimeSum, p.processingTimeCount
}

func NewRater(ctx context.Context, mv *v1alpha1.MonoVertex, opts ...Option) *Rater {
	rater := Rater{
		monoVertex: mv,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 1,
		},
		log:                          logging.FromContext(ctx).Named("Rater"),
		options:                      defaultOptions(),
		userSpecifiedLookBackSeconds: atomic.NewFloat64(float64(mv.Spec.Scale.GetLookbackSeconds())),
	}

	rater.podTracker = NewPodTracker(ctx, mv)
	// maintain the total counts of the last 30 minutes(1800 seconds) since we support 1m, 5m, 15m lookback seconds.
	rater.timestampedPodCounts = sharedqueue.New[*TimestampedCounts](int(1800 / CountWindow.Seconds()))
	rater.timestampedPodProcessingTime = sharedqueue.New[*TimestampedProcessingTime](int(1800 / CountWindow.Seconds()))

	for _, opt := range opts {
		if opt != nil {
			opt(rater.options)
		}
	}
	return &rater
}

// Function monitor() defines each of the worker's jobs.
// It waits for keys in the channel, and starts a monitoring job
func (r *Rater) monitor(ctx context.Context, id int, keyCh <-chan string) {
	r.log.Infof("Started monitoring worker %v", id)
	for {
		select {
		case <-ctx.Done():
			r.log.Infof("Stopped monitoring worker %v", id)
			return
		case key := <-keyCh:
			if err := r.monitorOnePod(ctx, key, id); err != nil {
				r.log.Errorw("Failed to monitor a pod", zap.String("pod", key), zap.Error(err))
			}
		}
	}
}

// monitorOnePod monitors a single pod and updates the rate metrics for the given pod.
func (r *Rater) monitorOnePod(ctx context.Context, key string, worker int) error {
	log := logging.FromContext(ctx).With("worker", fmt.Sprint(worker)).With("podKey", key)
	log.Debugf("Working on key: %s", key)
	pInfo, err := r.podTracker.GetPodInfo(key)
	if err != nil {
		return err
	}
	var podReadCount *PodReadCount
	var processingTime *PodProcessingTime
	if r.podTracker.IsActive(key) {
		podReadCount = r.getPodReadCounts(pInfo.podName)
		if podReadCount == nil {
			log.Debugf("Failed retrieving total podReadCounts for pod %s", pInfo.podName)
		}
		processingTime = r.getPodProcessingTime(pInfo.podName)
		if processingTime == nil {
			log.Debugf("Failed retrieving total processingTime for pod %s", pInfo.podName)
		}
	} else {
		log.Debugf("Pod %s does not exist, updating it with nil...", pInfo.podName)
		podReadCount = nil
		processingTime = nil
	}
	now := time.Now().Add(CountWindow).Truncate(CountWindow).Unix()
	UpdateCount(r.timestampedPodCounts, now, podReadCount)
	UpdateProcessingTime(r.timestampedPodProcessingTime, now, processingTime)
	r.log.Infof("MYDEBUG: processing rate vertex %s is: %v", pInfo.monoVertexName, r.timestampedPodProcessingTime)
	return nil
}

// getPodReadCounts returns the total number of messages read by the pod
// It fetches the total pod read counts from the Prometheus metrics endpoint.
func (r *Rater) getPodReadCounts(podName string) *PodReadCount {
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

	if value, ok := result[monoVtxReadMetricName]; ok && value != nil && len(value.GetMetric()) > 0 {
		metricsList := value.GetMetric()
		// Each pod should be emitting only one metric with this name, so we should be able to take the first value
		// from the results safely.
		// We use Untyped here as the counter metric family shows up as untyped from the rust client
		// TODO(MonoVertex): Check further on this to understand why not type is counter
		// https://github.com/prometheus/client_rust/issues/194
		podReadCount := &PodReadCount{podName, metricsList[0].Untyped.GetValue()}
		return podReadCount
	} else {
		r.log.Infof("[Pod name %s]: Metric %q is unavailable, the pod might haven't started processing data", podName, monoVtxReadMetricName)
		return nil
	}
}

// getPodProcessingTime
func (r *Rater) getPodProcessingTime(podName string) *PodProcessingTime {
	//processingTimeSumMetricName := "forwarder_forward_chunk_processing_time_sum"
	//processingTimeCountMetricName := "forwarder_forward_chunk_processing_time_count"
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
		r.log.Infof("[Pod name %s]: Metric %q is unavailable, the pod might haven't started processing data", podName, processingTimeCountMetric)
		return nil
	}
	return &PodProcessingTime{
		name:                podName,
		processingTimeSum:   podSum,
		processingTimeCount: podCount,
	}
}

// GetRates returns the rate metrics for the MonoVertex.
// It calculates the rate metrics for the given lookback seconds.
func (r *Rater) GetRates() map[string]*wrapperspb.DoubleValue {
	r.log.Debugf("Current timestampedPodCounts for MonoVertex %s is: %v", r.monoVertex.Name, r.timestampedPodCounts)
	var result = make(map[string]*wrapperspb.DoubleValue)
	// calculate rates for each lookback seconds
	for n, i := range r.buildLookbackSecondsMap() {
		if n == "default" {
			r.log.Infof("MYDEBUG: lookback %d", i)
		}
		rate := CalculateRate(r.timestampedPodCounts, i)
		result[n] = wrapperspb.Double(rate)
	}
	r.UpdateDynamicLookBack()
	r.log.Debugf("Got rates for MonoVertex %s: %v", r.monoVertex.Name, result)
	return result
}

func (r *Rater) buildLookbackSecondsMap() map[string]int64 {
	lbValue := r.userSpecifiedLookBackSeconds.Load()
	lookbackSecondsMap := map[string]int64{"default": int64(lbValue)}
	for k, v := range fixedLookbackSeconds {
		lookbackSecondsMap[k] = v
	}
	return lookbackSecondsMap
}

func (r *Rater) Start(ctx context.Context) error {
	r.log.Infof("Starting rater...")
	keyCh := make(chan string)
	ctx, cancel := context.WithCancel(logging.WithLogger(ctx, r.log))
	defer cancel()

	go func() {
		err := r.podTracker.Start(ctx)
		if err != nil {
			r.log.Errorw("Failed to start pod tracker", zap.Error(err))
		}
	}()

	// Worker group
	for i := 1; i <= r.options.workers; i++ {
		go r.monitor(ctx, i, keyCh)
	}

	// Function assign() sends the least recently used podKey to the channel so that it can be picked up by a worker.
	assign := func() {
		if e := r.podTracker.LeastRecentlyUsed(); e != "" {
			keyCh <- e
			return
		}
	}

	// Following for loop keeps calling assign() function to assign monitoring tasks to the workers.
	// It makes sure each element in the list will be assigned every N milliseconds.
	for {
		select {
		case <-ctx.Done():
			r.log.Info("Shutting down monitoring job assigner")
			return nil
		default:
			assign()
			// Make sure each of the key will be assigned at least every taskInterval milliseconds.
			sleep(ctx, time.Millisecond*time.Duration(func() int {
				l := r.podTracker.GetActivePodsCount()
				if l == 0 {
					return r.options.taskInterval
				}
				result := r.options.taskInterval / l
				if result > 0 {
					return result
				}
				return 1
			}()))
		}
	}
}

// sleep function uses a select statement to check if the context is canceled before sleeping for the given duration
// it helps ensure the sleep will be released when the context is canceled, allowing the goroutine to exit gracefully
func sleep(ctx context.Context, duration time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(duration):
	}
}

// UpdateDynamicLookBack updates the default lookback period of a vertex based on the processing rate
func (r *Rater) UpdateDynamicLookBack() map[string]*wrapperspb.DoubleValue {
	var result = make(map[string]*wrapperspb.DoubleValue)
	// calculate rates for each lookback seconds

	vertexName := r.monoVertex.Name
	pt := r.CalculateVertexProcessingTime(r.timestampedPodProcessingTime)
	// if the current calculated processing time is greater than the lookback Seconds, update it
	currentVal := r.userSpecifiedLookBackSeconds.Load()
	r.log.Infof("MYDEBUG: pt %f ", pt)
	// adding 30 seconds for buffer
	minute := 60*int(math.Round(pt/60)) + 30
	if pt > currentVal {
		// TODO(adapt): We should find a suitable value for this, using 2 * pt right now
		r.userSpecifiedLookBackSeconds.Store(float64(minute))
		r.log.Infof("MYDEBUG: Updated for vertex %s, old %f new %f", vertexName, currentVal, minute)
	} else {
		minute = int(math.Max(float64(minute), float64(r.monoVertex.Spec.Scale.GetLookbackSeconds())))
		// TODO(adapt): We should find a suitable value for this, using 2 * pt right now
		if minute != int(currentVal) {
			r.userSpecifiedLookBackSeconds.Store(float64(minute))
			r.log.Infof("MYDEBUG: Updated for vertex %s, old %f new %f", vertexName, currentVal, minute)
		}

	}

	return result
}

func (r *Rater) CalculateVertexProcessingTime(q *sharedqueue.OverflowQueue[*TimestampedProcessingTime]) float64 {
	counts := q.Items()
	currentLookback := r.userSpecifiedLookBackSeconds.Load()
	// If we do not have enough data points, lets send back the default from the vertex
	if len(counts) <= 1 {
		return currentLookback
	}
	startIndex := findStartIndexPt(int64(currentLookback*3), counts)
	// we consider the last but one element as the end index because the last element might be incomplete
	// we can be sure that the last but one element in the queue is complete.
	endIndex := len(counts) - 2
	// If we do not have data from previous timeline, then return the current
	// lookback time itself
	// This also acts as a gating where when lb * 3 > data window size, we will
	// increase the lb beyond that
	if startIndex == indexNotFound {
		return currentLookback
	}

	// time diff in seconds.
	timeDiff := counts[endIndex].timestamp - counts[startIndex].timestamp
	if timeDiff == 0 {
		// if the time difference is 0, we return 0 to avoid division by 0
		// this should not happen in practice because we are using a 10s interval
		return currentLookback
	}

	//delta := float64(0)
	//for i := startIndex; i < endIndex; i++ {
	//	// calculate the difference between the current and previous pod count snapshots
	//	delta += calculatePtDelta(counts[i], counts[i+1])
	//}

	cumulativeTime := make(map[string]float64)
	count := make(map[string]int)

	// Iterate over the range of items in the queue
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

	maxAverage := 0.0
	// Calculate averages and find the maximum
	for pod, totalTime := range cumulativeTime {
		if totalCnt := count[pod]; totalCnt > 0 {
			average := totalTime / float64(totalCnt)
			if average > maxAverage {
				maxAverage = average
			}
		}
	}
	// Return the maximum average processing time
	return maxAverage
}

// calculatePodDelta calculates the difference between the current and previous pod count snapshots
func calculatePtDelta(tc1, tc2 *TimestampedProcessingTime) float64 {
	delta := float64(0)
	if tc1 == nil || tc2 == nil {
		// we calculate delta only when both input timestamped counts are non-nil
		return delta
	}
	prevPodReadCount := tc1.PodProcessingTimeSnapshot()
	currPodReadCount := tc2.PodProcessingTimeSnapshot()
	for podName, readCount := range currPodReadCount {
		currCount := readCount
		prevCount := prevPodReadCount[podName]
		// pod delta will be equal to current count in case of restart
		podDelta := currCount
		if currCount >= prevCount {
			podDelta = currCount - prevCount
		}
		delta += podDelta
	}
	return delta
}
