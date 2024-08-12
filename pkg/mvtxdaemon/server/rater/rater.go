package rater

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

const CountWindow = time.Second * 10

// metricsHttpClient interface for the GET/HEAD call to metrics endpoint.
// Had to add this an interface for testing
type metricsHttpClient interface {
	Get(url string) (*http.Response, error)
	Head(url string) (*http.Response, error)
}

// Rater is a struct that maintains information about the processing rate of each vertex.
// It monitors the number of processed messages for each pod in a vertex and calculates the rate.
type Rater struct {
	monoVertex *v1alpha1.MonoVertex
	httpClient metricsHttpClient
	log        *zap.SugaredLogger
	podTracker *PodTracker
	// timestampedPodCounts is a map between vertex name and a queue of timestamped counts for that vertex
	timestampedPodCounts map[int]*sharedqueue.OverflowQueue[*util.TimestampedCounts]
	// userSpecifiedLookBackSeconds is a map between vertex name and the user-specified lookback seconds for that vertex
	userSpecifiedLookBackSeconds map[int]int64
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
		timestampedPodCounts:         make(map[int]*sharedqueue.OverflowQueue[*util.TimestampedCounts]),
		userSpecifiedLookBackSeconds: make(map[int]int64),
		options:                      defaultOptions(),
	}

	rater.podTracker = NewPodTracker(ctx, mv)
	for i := 0; i < mv.GetReplicas(); i++ {
		// maintain the total counts of the last 30 minutes(1800 seconds) since we support 1m, 5m, 15m lookback seconds.
		rater.timestampedPodCounts[i] = sharedqueue.New[*util.TimestampedCounts](int(1800 / CountWindow.Seconds()))
		rater.userSpecifiedLookBackSeconds[i] = int64(mv.Spec.Scale.GetLookbackSeconds())
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
	pInfo, err := r.podTracker.GetPodInfo(key)
	if err != nil {
		return err
	}
	var podReadCount *PodReadCount
	if r.podTracker.IsActive(key) {
		podReadCount = r.getPodReadCounts(pInfo.replica, pInfo.podName)
		if podReadCount == nil {
			log.Debugf("Failed retrieving total podReadCount for pod %s", pInfo.podName)
		}
	} else {
		log.Debugf("Pod %s does not exist, updating it with nil...", pInfo.podName)
		podReadCount = nil
	}
	now := time.Now().Add(CountWindow).Truncate(CountWindow).Unix()
	UpdateCount(r.timestampedPodCounts[pInfo.replica], now, podReadCount)
	return nil
}

// getPodReadCounts returns the total number of messages read by the pod
// since a pod can read from multiple partitions, we will return a map of partition to read count.
func (r *Rater) getPodReadCounts(vertexName, podName string) *PodReadCount {
	readTotalMetricName := "forwarder_data_read_total"

	// scrape the read total metric from pod metric port
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/metrics", podName, r.pipeline.Name+"-"+vertexName+"-headless", r.pipeline.Namespace, v1alpha1.VertexMetricsPort)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		r.log.Errorf("[vertex name %s, pod name %s]: failed reading the metrics endpoint, %v", vertexName, podName, err.Error())
		return nil
	}
	defer resp.Body.Close()

	textParser := expfmt.TextParser{}
	result, err := textParser.TextToMetricFamilies(resp.Body)
	if err != nil {
		r.log.Errorf("[vertex name %s, pod name %s]: failed parsing to prometheus metric families, %v", vertexName, podName, err.Error())
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
				r.log.Warnf("[vertex name %s, pod name %s]: Partition name is not found for metric %s", vertexName, podName, readTotalMetricName)
			} else {
				partitionReadCount[partitionName] = ele.Counter.GetValue()
			}
		}
		podReadCount := &PodReadCount{podName, partitionReadCount}
		return podReadCount
	} else {
		r.log.Errorf("[vertex name %s, pod name %s]: failed getting the read total metric, the metric is not available.", vertexName, podName)
		return nil
	}
}

// GetRates returns the processing rates of the vertex partition in the format of lookback second to rate mappings
func (r *Rater) GetRates(vertexName, partitionName string) map[string]*wrapperspb.DoubleValue {
	r.log.Debugf("Getting rates for vertex %s, partition %s", vertexName, partitionName)
	r.log.Debugf("Current timestampedPodCounts for vertex %s is: %v", vertexName, r.timestampedPodCounts[vertexName])
	var result = make(map[string]*wrapperspb.DoubleValue)
	// calculate rates for each lookback seconds
	for n, i := range r.buildLookbackSecondsMap(vertexName) {
		r := CalculateRate(r.timestampedPodCounts[vertexName], i, partitionName)
		result[n] = wrapperspb.Double(r)
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
