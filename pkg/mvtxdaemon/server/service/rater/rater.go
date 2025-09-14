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
	"io"
	"math"
	"net/http"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const monoVtxReadMetricName = "monovtx_read_total"
const monoVtxPendingRawMetric = "monovtx_pending_raw"

// MaxLookback is the upper limit beyond which lookback value is not increased
// by the dynamic algorithm. This is chosen as a conservative limit
// where vertices taking beyond this time for processing might not be the
// best candidate for auto-scaling. Might be prudent to keep fixed pods at that time.

// MonoVtxRatable is the interface for the Rater struct.
type MonoVtxRatable interface {
	Start(ctx context.Context) error
	GetRates() map[string]*wrapperspb.DoubleValue
	GetPending() map[string]*wrapperspb.Int64Value
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
	// timestampedPendingCount is a queue of timestamped pending counts for the MonoVertex
	timestampedPendingCount *sharedqueue.OverflowQueue[*TimestampedCounts]
	// lookBackSeconds is the lookback time window used for scaling calculations
	// this can be updated dynamically, defaults to user-specified value in the spec
	lookBackSeconds *atomic.Float64
	options         *options
}

// PodMetricsCount is a struct to maintain count of messages read by a pod of MonoVertex
type PodMetricsCount struct {
	// pod name of the pod
	name string
	// represents the count of messages read by the pod
	readCount float64
}

// Name returns the pod name
func (p *PodMetricsCount) Name() string {
	return p.name
}

// ReadCount returns the value of the messages read by the Pod
func (p *PodMetricsCount) ReadCount() float64 {
	return p.readCount
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
		log:     logging.FromContext(ctx).Named("Rater"),
		options: defaultOptions(),
		// load the default lookback value from the spec
		lookBackSeconds: atomic.NewFloat64(float64(mv.Spec.Scale.GetLookbackSeconds())),
	}
	rater.podTracker = NewPodTracker(ctx, mv)
	// maintain the total counts of the last 30 minutes(1800 seconds) since we support 1m, 5m, 15m lookback seconds.
	rater.timestampedPodCounts = sharedqueue.New[*TimestampedCounts](360)
	// maintain the total pending counts of the last 30 minutes(1800 seconds) since we support 1m, 5m, 15m lookback seconds.
	rater.timestampedPendingCount = sharedqueue.New[*TimestampedCounts](360)
	for _, opt := range opts {
		if opt != nil {
			opt(rater.options)
		}
	}
	// initialise the metric value for the lookback window
	metrics.MonoVertexLookBackSecs.WithLabelValues(mv.Name).Set(rater.lookBackSeconds.Load())
	return &rater
}

// podTask represents a monitoring task for a pod with a common timestamp
type podTask struct {
	key       string
	timestamp int64
}

// Function monitor() defines each of the worker's jobs.
// It waits for pod monitoring tasks in the channel, and starts them
func (r *Rater) monitor(ctx context.Context, id int, taskCh <-chan *podTask) {
	r.log.Infof("Started monitoring worker %v", id)
	for {
		select {
		case <-ctx.Done():
			r.log.Infof("Stopped monitoring worker %v", id)
			return
		case task := <-taskCh:
			if err := r.monitorOnePod(ctx, task.key, id, task.timestamp); err != nil {
				r.log.Errorw("Failed to monitor a pod", zap.String("pod", task.key), zap.Error(err))
			}
		}
	}
}

// monitorOnePod monitors a single pod and updates the rate metrics for the given pod.
func (r *Rater) monitorOnePod(ctx context.Context, key string, worker int, timestamp int64) error {
	log := logging.FromContext(ctx).With("worker", fmt.Sprint(worker)).With("podKey", key)
	log.Debugf("Working on key: %s", key)
	pInfo, err := r.podTracker.GetPodInfo(key)
	if err != nil {
		return err
	}
	var podReadCount, podPendingCount *PodMetricsCount

	if !r.podTracker.IsActive(key) {
		log.Debugf("Pod %s does not exist, updating it with nil...", pInfo.podName)
	} else {
		podMetrics := r.getPodMetrics(pInfo.podName)
		if podMetrics == nil {
			log.Debugf("No metrics available for pod %s", pInfo.podName)
		} else {
			podReadCount = r.getPodReadCounts(pInfo.podName, podMetrics)
			if podReadCount == nil {
				log.Debugf("Failed retrieving read counts for pod %s", pInfo.podName)
			}
			// Pending is only fetched from replica 0. Only maintain that in the timeline
			if pInfo.replica == 0 {
				podPendingCount = r.getPodPendingCounts(pInfo.podName, podMetrics)
				if podPendingCount == nil {
					log.Debugf("Failed retrieving pending counts for pod %s", pInfo.podName)
				}
				UpdateCount(r.timestampedPendingCount, timestamp, podPendingCount)
			}
		}
	}
	UpdateCount(r.timestampedPodCounts, timestamp, podReadCount)
	return nil
}

// getPodMetrics Fetches the metrics for a given pod from the Prometheus metrics endpoint.
func (r *Rater) getPodMetrics(podName string) map[string]*dto.MetricFamily {
	headlessServiceName := r.monoVertex.GetHeadlessServiceName()
	// scrape the read total metric from pod metric port
	// example for 0th pod: https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless.default.svc:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/metrics", podName, headlessServiceName, r.monoVertex.Namespace, v1alpha1.MonoVertexMetricsPort)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		r.log.Warnf("[Pod name %s]: failed reading the metrics endpoint, the pod might have been scaled down: %v", podName, err.Error())
		return nil
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			r.log.Errorf("Failed to close response body for pod %s: %v", podName, err)
		}
	}(resp.Body)

	textParser := expfmt.TextParser{}
	result, err := textParser.TextToMetricFamilies(resp.Body)
	if err != nil {
		r.log.Errorf("[Pod name %s]: failed parsing to prometheus metric families, %v", podName, err)
		return nil
	}
	return result
}

// getPodReadCounts returns the total number of messages read by the pod
func (r *Rater) getPodReadCounts(podName string, result map[string]*dto.MetricFamily) *PodMetricsCount {
	if value, ok := result[monoVtxReadMetricName]; ok && value != nil && len(value.GetMetric()) > 0 {
		metricsList := value.GetMetric()
		// Each pod should be emitting only one metric with this name, so we should be able to take the first value
		// from the results safely.
		// We use Untyped here as the counter metric family shows up as untyped from the rust client
		// TODO(MonoVertex): Check further on this to understand why not type is counter
		// https://github.com/prometheus/client_rust/issues/194
		podReadCount := &PodMetricsCount{podName, metricsList[0].Untyped.GetValue()}
		return podReadCount
	} else {
		r.log.Infof("[Pod name %s]: Metric %q is unavailable, the pod might haven't started processing data", podName, monoVtxReadMetricName)
		return nil
	}
}

// getPodPendingCounts returns the total number of pending messages for a pod
func (r *Rater) getPodPendingCounts(podName string, result map[string]*dto.MetricFamily) *PodMetricsCount {
	if value, ok := result[monoVtxPendingRawMetric]; ok && value != nil && len(value.GetMetric()) > 0 {
		metricsList := value.GetMetric()
		podPendingCount := &PodMetricsCount{podName, metricsList[0].Gauge.GetValue()}
		return podPendingCount
	} else {
		r.log.Infof("[Pod name %s]: Metric %q is unavailable, the pod might haven't started processing data", podName, monoVtxPendingRawMetric)
	}
	return nil
}

// GetPending returns the pending count for the mono vertex
func (r *Rater) GetPending() map[string]*wrapperspb.Int64Value {
	r.log.Debugf("Current timestampedPendingCount for MonoVertex %s is: %v", r.monoVertex.Name, r.timestampedPendingCount)
	var result = make(map[string]*wrapperspb.Int64Value)
	// calculate pending for each lookback seconds
	for windowLabel, lookbackSeconds := range r.buildLookbackSecondsMap() {
		pending := CalculatePending(r.timestampedPendingCount, lookbackSeconds)
		result[windowLabel] = wrapperspb.Int64(pending)
		// Expose the metric for pending
		if pending != v1alpha1.PendingNotAvailable {
			metrics.MonoVertexPendingMessages.WithLabelValues(r.monoVertex.Name, windowLabel).Set(float64(pending))
		}
	}
	r.log.Debugf("Got Pending for MonoVertex %s: %v", r.monoVertex.Name, result)
	return result
}

// GetRates returns the rate metrics for the MonoVertex.
// It calculates the rate metrics for the given lookback seconds.
func (r *Rater) GetRates() map[string]*wrapperspb.DoubleValue {
	r.log.Debugf("Current timestampedPodCounts for MonoVertex %s is: %v", r.monoVertex.Name, r.timestampedPodCounts)
	var result = make(map[string]*wrapperspb.DoubleValue)
	// calculate rates for each lookback seconds
	for windowLabel, lookbackSeconds := range r.buildLookbackSecondsMap() {
		rate := CalculateRate(r.timestampedPodCounts, lookbackSeconds)
		result[windowLabel] = wrapperspb.Double(rate)
	}
	r.log.Debugf("Got rates for MonoVertex %s: %v", r.monoVertex.Name, result)
	return result
}

func (r *Rater) buildLookbackSecondsMap() map[string]int64 {
	// as the default lookback value can be changing dynamically,
	// load the current value for the lookback seconds
	lbValue := r.lookBackSeconds.Load()
	lookbackSecondsMap := map[string]int64{"default": int64(lbValue)}
	for k, v := range fixedLookbackSeconds {
		lookbackSecondsMap[k] = v
	}
	return lookbackSecondsMap
}

func (r *Rater) Start(ctx context.Context) error {
	r.log.Infof("Starting rater...")
	taskCh := make(chan *podTask)
	ctx, cancel := context.WithCancel(logging.WithLogger(ctx, r.log))
	defer cancel()

	go func() {
		err := r.podTracker.Start(ctx)
		if err != nil {
			r.log.Errorw("Failed to start pod tracker", zap.Error(err))
		}
	}()

	// start the dynamic lookback check which will be
	// updating the lookback period based on the data read time.
	go r.startDynamicLookBack(ctx)

	// Worker group
	for i := 1; i <= r.options.workers; i++ {
		go r.monitor(ctx, i, taskCh)
	}

	// Function assign() sends the least recently used podKey to the channel so that it can be picked up by a worker.
	assign := func(timestamp int64) {
		if e := r.podTracker.LeastRecentlyUsed(); e != "" {
			select {
			case taskCh <- &podTask{key: e, timestamp: timestamp}:
			case <-ctx.Done():
			}
		}
	}

	ticker := time.NewTicker(time.Duration(r.options.taskInterval) * time.Millisecond)
	// Following for loop keeps calling assign() function to assign monitoring tasks to the workers.
	// It makes sure each element in the list will be assigned every N milliseconds.
	for {
		select {
		case <-ctx.Done():
			r.log.Info("Shutting down monitoring job assigner")
			return nil
		case <-ticker.C:
			// Generate a common timestamp for all pods in this tick
			now := time.Now().Unix()
			for range r.podTracker.GetActivePodsCount() {
				assign(now)
			}
		}
	}
}

func (r *Rater) startDynamicLookBack(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	// Ensure the ticker is stopped to prevent a resource leak.
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.updateDynamicLookbackSecs()
		case <-ctx.Done():
			// If the context is canceled or expires exit
			return
		}
	}
}

// updateDynamicLookbackSecs continuously adjusts ths lookback duration based on the current
// processing time of the MonoVertex system.
func (r *Rater) updateDynamicLookbackSecs() {
	counts := r.timestampedPodCounts.Items()
	if len(counts) <= 1 {
		return
	}
	// We will calculate the processing time for a time window = 3 * currentLookback
	// This ensures that we have enough data to capture one complete processing
	currentLookback := r.lookBackSeconds.Load()
	startIndex := findStartIndex(3*int64(currentLookback), counts)
	// we consider the last but one element as the end index because the last element might be incomplete
	// we can be sure that the last but one element in the queue is complete.
	endIndex := len(counts) - 2
	if startIndex == indexNotFound {
		return
	}

	// time diff in seconds.
	timeDiff := counts[endIndex].timestamp - counts[startIndex].timestamp
	if timeDiff == 0 {
		// no action required here
		return
	}
	maxProcessingTime := CalculateMaxLookback(counts, startIndex, endIndex)
	// round up to the nearest minute, also ensure that while going up and down we have the consistent value for
	// a given processingTimeSeconds, then convert back to seconds
	roundedMaxLookback := 60.0 * (math.Ceil(float64(maxProcessingTime) / 60.0))
	// Based on the value received we can have two cases
	// 1. Step up case (value is > than current):
	// 	  Do not allow the value to be increased more than the MaxLookback allowed (10mins)
	// 2. Step Down (value is <= than current)
	//    Do not allow the value to be lower the lookback value specified in the spec
	if roundedMaxLookback > currentLookback {
		roundedMaxLookback = math.Min(roundedMaxLookback, float64(v1alpha1.MaxLookbackSeconds))
	} else {
		roundedMaxLookback = math.Max(roundedMaxLookback, float64(r.monoVertex.Spec.Scale.GetLookbackSeconds()))
	}
	// If the value has changed, update it
	if roundedMaxLookback != currentLookback {
		r.lookBackSeconds.Store(roundedMaxLookback)
		r.log.Debugf("Lookback updated for mvtx: %s, Current: %f Updated: %f", r.monoVertex.Name, currentLookback, roundedMaxLookback)
		// update the metric value for the lookback window
		metrics.MonoVertexLookBackSecs.WithLabelValues(r.monoVertex.Name).Set(roundedMaxLookback)
	}
}
