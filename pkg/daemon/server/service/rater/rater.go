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
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

type Ratable interface {
	Start(ctx context.Context) error
	GetRates(vertexName, partitionName string) map[string]float64
}

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
	// userSpecifiedLookBackSeconds is a map between vertex name and the user-specified lookback seconds for that vertex
	userSpecifiedLookBackSeconds map[string]int64
	options                      *options
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
		log:                          logging.FromContext(ctx).Named("Rater"),
		timestampedPodCounts:         make(map[string]*sharedqueue.OverflowQueue[*TimestampedCounts]),
		userSpecifiedLookBackSeconds: make(map[string]int64),
		options:                      defaultOptions(),
	}

	rater.podTracker = NewPodTracker(ctx, p)
	for _, v := range p.Spec.Vertices {
		// maintain the total counts of the last 30 minutes(1800 seconds) since we support 1m, 5m, 15m lookback seconds.
		rater.timestampedPodCounts[v.Name] = sharedqueue.New[*TimestampedCounts](int(1800 / CountWindow.Seconds()))
		rater.userSpecifiedLookBackSeconds[v.Name] = int64(v.Scale.GetLookbackSeconds())
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
	podInfo := strings.Split(key, PodInfoSeparator)
	if len(podInfo) != 3 {
		return fmt.Errorf("invalid key %q", key)
	}
	vertexName := podInfo[1]
	podName := strings.Join([]string{podInfo[0], podInfo[1], podInfo[2]}, "-")
	var podReadCount *PodReadCount
	if r.podTracker.IsActive(key) {
		podReadCount = r.getPodReadCounts(vertexName, podName)
		if podReadCount == nil {
			log.Debugf("Failed retrieving total podReadCount for pod %s", podName)
		}
	} else {
		log.Debugf("Pod %s does not exist, updating it with nil...", podName)
		podReadCount = nil
	}
	now := time.Now().Add(CountWindow).Truncate(CountWindow).Unix()
	UpdateCount(r.timestampedPodCounts[vertexName], now, podReadCount)
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

// getPodReadCounts returns the total number of messages read by the pod
// since a pod can read from multiple partitions, we will return a map of partition to read count.
func (r *Rater) getPodReadCounts(vertexName, podName string) *PodReadCount {
	readTotalMetricName := "forwarder_data_read_total"

	// scrape the read total metric from pod metric port
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/metrics", podName, r.pipeline.Name+"-"+vertexName+"-headless", r.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		r.log.Errorf("failed reading the metrics endpoint, %v", err.Error())
		return nil
	}
	defer resp.Body.Close()

	textParser := expfmt.TextParser{}
	result, err := textParser.TextToMetricFamilies(resp.Body)
	if err != nil {
		r.log.Errorf("failed parsing to prometheus metric families, %v", err.Error())
		return nil
	}

	if value, ok := result[readTotalMetricName]; ok && value != nil && len(value.GetMetric()) > 0 {
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
				r.log.Warnf("Partition name is not found for metric %s", readTotalMetricName)
			} else {
				partitionReadCount[partitionName] = ele.Counter.GetValue()
			}
		}
		podReadCount := &PodReadCount{podName, partitionReadCount}
		return podReadCount
	} else {
		r.log.Errorf("failed getting the read total metric, the metric is not available.")
		return nil
	}
}

// GetRates returns the processing rates of the vertex partition in the format of lookback second to rate mappings
func (r *Rater) GetRates(vertexName, partitionName string) map[string]float64 {
	r.log.Debugf("Getting rates for vertex %s, partition %s", vertexName, partitionName)
	r.log.Debugf("Current timestampedPodCounts for vertex %s is: %v", vertexName, r.timestampedPodCounts[vertexName])
	var result = make(map[string]float64)
	// calculate rates for each lookback seconds
	for n, i := range r.buildLookbackSecondsMap(vertexName) {
		r := CalculateRate(r.timestampedPodCounts[vertexName], i, partitionName)
		result[n] = r
	}
	r.log.Debugf("Got rates for vertex %s, partition %s: %v", vertexName, partitionName, result)
	return result
}

func (r *Rater) buildLookbackSecondsMap(vertexName string) map[string]int64 {
	lookbackSecondsMap := map[string]int64{"default": r.userSpecifiedLookBackSeconds[vertexName]}
	for k, v := range fixedLookbackSeconds {
		lookbackSecondsMap[k] = v
	}
	return lookbackSecondsMap
}
