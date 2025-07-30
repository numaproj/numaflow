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
	"maps"
	"math"
	"net/http"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

type Ratable interface {
	Start(ctx context.Context) error
	GetRates(vertexName, partitionName string) map[string]*wrapperspb.DoubleValue
	GetPending(pipelineName, vertexName, vertexType, partitionName string) map[string]*wrapperspb.Int64Value
}

var _ Ratable = (*Rater)(nil)

// CountWindow is the time window for which we maintain the timestamped counts, currently 10 seconds
// e.g., if the current time is 12:00:07,
// the retrieved count will be tracked in the 12:00:00-12:00:10 time window using 12:00:10 as the timestamp
const CountWindow = time.Second * 10

// metricsHttpClient interface for the GET/HEAD call to metrics endpoint.
// Had to add this an interface for testing
type metricsHttpClient interface {
	Get(url string) (*http.Response, error)
	Head(url string) (*http.Response, error)
}

// fixedLookbackSeconds always maintain rate metrics for the following lookback seconds (1m, 5m, 15m)
var fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}

// Rater is a struct that maintains information about the processing rate of each vertex.
// It monitors the number of processed messages for each pod in a vertex and calculates the rate.
type Rater struct {
	pipeline   *v1alpha1.Pipeline
	httpClient metricsHttpClient
	log        *zap.SugaredLogger
	podTracker *PodTracker
	// timestampedPodCounts is a map between vertex name and a queue of timestamped counts for that vertex
	timestampedPodCounts map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]
	// timestampedPendingCount is a map between vertex name and a queue of timestamped counts for pending messages
	timestampedPendingCount map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]
	// this can be updated dynamically, defaults to user-specified value in the spec
	lookBackSeconds map[string]*atomic.Float64
	options         *options
}

type PodPendingCount struct {
	// pod name
	name string
	// key represents partition name, value represents the count of messages pending in the corresponding partition
	partitionPendingCounts map[string]float64
}

func (p *PodPendingCount) Name() string {
	return p.name
}

func (p *PodPendingCount) PartitionPendingCounts() map[string]float64 {
	return p.partitionPendingCounts
}

// PodReadCount is a struct to maintain count of messages read from each partition by a pod
type PodReadCount struct {
	// pod name
	name string
	// key represents partition name, value represents the count of messages read by the corresponding partition
	partitionReadCounts map[string]float64
}

func (p *PodReadCount) Name() string {
	return p.name
}

func (p *PodReadCount) PartitionReadCounts() map[string]float64 {
	return p.partitionReadCounts
}

func NewRater(ctx context.Context, p *v1alpha1.Pipeline, opts ...Option) *Rater {
	rater := Rater{
		pipeline: p,
		httpClient: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
			Timeout: time.Second * 1,
		},
		log:                     logging.FromContext(ctx).Named("Rater"),
		timestampedPodCounts:    make(map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]),
		timestampedPendingCount: make(map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]),
		lookBackSeconds:         make(map[string]*atomic.Float64),
		options:                 defaultOptions(),
	}

	rater.podTracker = NewPodTracker(ctx, p)
	for _, v := range p.Spec.Vertices {
		// maintain the total counts of the last 30 minutes(1800 seconds) since we support 1m, 5m, 15m lookback seconds.
		rater.timestampedPodCounts[v.Name] = sharedqueue.New[*TimestampedCounts](int(1800 / CountWindow.Seconds()))
		// maintain the pending counts of the last 30 minutes(1800 seconds) since we support 1m, 5m, 15m lookback seconds.
		rater.timestampedPendingCount[v.Name] = sharedqueue.New[*TimestampedCounts](int(1800 / CountWindow.Seconds()))
		rater.lookBackSeconds[v.Name] = atomic.NewFloat64(float64(v.Scale.GetLookbackSeconds()))
		// initialise the metric value for the lookback window for each vertex
		metrics.VertexLookBackSecs.WithLabelValues(v.Name, string(v.GetVertexType())).Set(float64(rater.lookBackSeconds[v.Name].Load()))
	}

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

func (r *Rater) monitorOnePod(ctx context.Context, key string, worker int) error {
	log := logging.FromContext(ctx).With("worker", fmt.Sprint(worker)).With("podKey", key)
	log.Debugf("Working on key: %s", key)
	podInfo, err := r.podTracker.GetPodInfo(key)
	vtx := r.pipeline.GetVertex(podInfo.vertexName)
	isReduce := vtx.IsReduceUDF()
	if err != nil {
		return err
	}
	var podReadCount *PodReadCount
	var podPendingCount *PodPendingCount
	now := time.Now().Add(CountWindow).Truncate(CountWindow).Unix()
	if r.podTracker.IsActive(key) {
		podMetrics := r.getPodMetrics(podInfo.vertexName, podInfo.podName)
		podReadCount = r.getPodReadCounts(podInfo.vertexName, podInfo.podName, podMetrics)
		if podReadCount == nil {
			log.Debugf("Failed retrieving total podReadCount for pod %s", podInfo.podName)
		}
		// Only maintain the timestamped pending counts if pod is a Reduce or if it is the 0th replica of any other vertex type.
		if isReduce || podInfo.replica == 0 {
			podPendingCount = r.getPodPendingCounts(podInfo.vertexName, podInfo.podName, podMetrics)
			if podPendingCount == nil {
				log.Debugf("Failed retrieving pending counts for pod %s vertex %s", podInfo.podName, podInfo.vertexName)
			}
			UpdatePendingCount(r.timestampedPendingCount[podInfo.vertexName], now, podPendingCount)
		}
	} else {
		log.Debugf("Pod %s does not exist, updating it with nil...", podInfo.podName)
	}
	UpdateCount(r.timestampedPodCounts[podInfo.vertexName], now, podReadCount)
	return nil
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

	// start the dynamic lookback check which will be
	// updating the lookback period based on the data read time.
	go r.startDynamicLookBack(ctx)

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

// getPodMetrics Fetches the metrics for a given pod from the Prometheus metrics endpoint.
func (r *Rater) getPodMetrics(vertexName, podName string) map[string]*dto.MetricFamily {
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/metrics", podName, r.pipeline.Name+"-"+vertexName+"-headless", r.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		r.log.Warnf("[vertex name %s, pod name %s]: failed reading the metrics endpoint, the pod might have been scaled down: %v", vertexName, podName, err.Error())
		return nil
	}
	defer resp.Body.Close()

	textParser := expfmt.TextParser{}
	result, err := textParser.TextToMetricFamilies(resp.Body)
	if err != nil {
		r.log.Errorf("[Pod name %s]:  failed parsing to prometheus metric families, %v", podName, err.Error())
		return nil
	}
	return result
}

// getPodPendingCounts returns the partition pending counts for a given pod
func (r *Rater) getPodPendingCounts(vertexName, podName string, metricsData map[string]*dto.MetricFamily) *PodPendingCount {
	pendingMetricName := "vertex_pending_messages_raw"
	if value, ok := metricsData[pendingMetricName]; ok && value != nil && len(value.GetMetric()) > 0 {
		metricsList := value.GetMetric()
		partitionPendingCount := make(map[string]float64)
		for _, ele := range metricsList {
			var partitionName string
			for _, label := range ele.Label {
				if label.GetName() == metrics.LabelPartitionName {
					partitionName = label.GetValue()
					break
				}
			}
			if partitionName == "" {
				r.log.Warnf("[vertex name %s, pod name %s]: Partition name is not found for metric %s", vertexName, podName, pendingMetricName)
			} else {
				gaugeVal := ele.Gauge.GetValue()
				untypedVal := ele.Untyped.GetValue()
				// Prefer Gauge, fallback to Untyped if Gauge is 0 but Untyped is not 0
				if gaugeVal == 0 && untypedVal != 0 {
					gaugeVal = untypedVal
				}
				partitionPendingCount[partitionName] = gaugeVal
			}
		}
		podPendingCount := &PodPendingCount{podName, partitionPendingCount}
		return podPendingCount
	} else {
		r.log.Warnf("[vertex name %s, pod name %s]: Metric %q is unavailable, the pod might haven't started processing data", vertexName, podName, pendingMetricName)
		return nil
	}
}

// getPodReadCounts returns the total number of messages read by the pod
// since a pod can read from multiple partitions, we will return a map of partition to read count.
func (r *Rater) getPodReadCounts(vertexName, podName string, metricsData map[string]*dto.MetricFamily) *PodReadCount {
	readTotalMetricName := "forwarder_data_read_total"
	if value, ok := metricsData[readTotalMetricName]; ok && value != nil && len(value.GetMetric()) > 0 {
		metricsList := value.GetMetric()
		partitionReadCount := make(map[string]float64)
		for _, ele := range metricsList {
			var partitionName string
			for _, label := range ele.Label {
				if label.GetName() == metrics.LabelPartitionName {
					partitionName = label.GetValue()
					break
				}
			}
			if partitionName == "" {
				r.log.Warnf("[vertex name %s, pod name %s]: Partition name is not found for metric %s", vertexName, podName, readTotalMetricName)
			} else {
				// https://github.com/prometheus/client_rust/issues/194
				counterVal := ele.Counter.GetValue()
				untypedVal := ele.Untyped.GetValue()
				if counterVal == 0 && untypedVal != 0 {
					counterVal = untypedVal
				}
				partitionReadCount[partitionName] = counterVal
			}
		}
		podReadCount := &PodReadCount{podName, partitionReadCount}
		return podReadCount
	} else {
		r.log.Infof("[vertex name %s, pod name %s]: Metric %q is unavailable, the pod might haven't started processing data", vertexName, podName, readTotalMetricName)
		return nil
	}
}

// GetRates returns the processing rates of the vertex partition in the format of lookback second to rate mappings
func (r *Rater) GetRates(vertexName, partitionName string) map[string]*wrapperspb.DoubleValue {
	r.log.Debugf("Getting rates for vertex %s, partition %s", vertexName, partitionName)
	r.log.Debugf("Current timestampedPodCounts for vertex %s is: %v", vertexName, r.timestampedPodCounts[vertexName])
	var result = make(map[string]*wrapperspb.DoubleValue)
	// calculate rates for each lookback seconds
	for windowLabel, lookbackSeconds := range r.buildLookbackSecondsMap(vertexName) {
		r := CalculateRate(r.timestampedPodCounts[vertexName], lookbackSeconds, partitionName)
		result[windowLabel] = wrapperspb.Double(r)
	}
	r.log.Debugf("Got rates for vertex %s, partition %s: %v", vertexName, partitionName, result)
	return result
}

// GetPending returns the pending count for the vertex partition in the format of lookback second to pending mappings
func (r *Rater) GetPending(pipelineName, vertexName, vertexType, partitionName string) map[string]*wrapperspb.Int64Value {
	r.log.Debugf("Current timestampedPendingCount for vertex %s is: %v", vertexName, r.timestampedPendingCount[vertexName])
	var result = make(map[string]*wrapperspb.Int64Value)
	// calculate pending for each lookback seconds
	for windowLabel, lookbackSeconds := range r.buildLookbackSecondsMap(vertexName) {
		pending := CalculatePending(r.timestampedPendingCount[vertexName], lookbackSeconds, partitionName)
		result[windowLabel] = wrapperspb.Int64(pending)
		// Expose the metric for pending
		if pending != isb.PendingNotAvailable {
			metrics.VertexPendingMessages.WithLabelValues(pipelineName, vertexName, vertexType, partitionName, windowLabel).Set(float64(pending))
		}
	}
	return result
}

// buildLookbackSecondsMap builds a map of lookback seconds for the given vertex
func (r *Rater) buildLookbackSecondsMap(vertexName string) map[string]int64 {
	// as the default lookback value can be changing dynamically,
	// load the current value for the lookback seconds
	lbValue := r.lookBackSeconds[vertexName].Load()
	lookbackSecondsMap := map[string]int64{"default": int64(lbValue)}
	maps.Copy(lookbackSecondsMap, fixedLookbackSeconds)
	return lookbackSecondsMap
}

// startDynamicLookBack starts the dynamic lookback adjustment process
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

// updateDynamicLookbackSecs updates the dynamic lookback seconds for each vertex
func (r *Rater) updateDynamicLookbackSecs() {
	for _, v := range r.pipeline.Spec.Vertices {
		vertexType := v.GetVertexType()
		vertexName := v.Name
		counts := r.timestampedPodCounts[vertexName].Items()
		if len(counts) <= 1 {
			return
		}
		currentLookback := r.lookBackSeconds[vertexName].Load()
		startIndex := findStartIndex(3*int64(currentLookback), counts)
		endIndex := len(counts) - 2
		if startIndex == indexNotFound {
			return
		}
		timeDiff := counts[endIndex].timestamp - counts[startIndex].timestamp
		if timeDiff == 0 {
			return
		}
		calculatedProcessingTime := CalculateLookback(counts, startIndex, endIndex)
		roundedCalculatedLookback := 60.0 * (math.Ceil(float64(calculatedProcessingTime) / 60.0))
		if roundedCalculatedLookback > currentLookback {
			roundedCalculatedLookback = math.Min(roundedCalculatedLookback, float64(v1alpha1.MaxLookbackSeconds))
		} else {
			roundedCalculatedLookback = math.Max(roundedCalculatedLookback, float64(v.Scale.GetLookbackSeconds()))
		}
		if roundedCalculatedLookback != currentLookback {
			r.lookBackSeconds[vertexName].Store(roundedCalculatedLookback)
			r.log.Infof("Lookback period updated for vertex %s, Current: %f Updated %f", vertexName, currentLookback, roundedCalculatedLookback)
			metrics.VertexLookBackSecs.WithLabelValues(vertexName, string(vertexType)).Set(roundedCalculatedLookback)
		}
	}
}
