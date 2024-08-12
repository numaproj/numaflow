package rater

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
)

const CountWindow = time.Second * 10

type MonoVtxRatable interface {
	Start(ctx context.Context) error
	GetRates(replicaIdx int) map[string]*wrapperspb.DoubleValue
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

// Rater is a struct that maintains information about the processing rate of each vertex.
// It monitors the number of processed messages for each pod in a vertex and calculates the rate.
type Rater struct {
	monoVertex *v1alpha1.MonoVertex
	httpClient metricsHttpClient
	log        *zap.SugaredLogger
	podTracker *PodTracker
	// timestampedPodCounts is a map between vertex name and a queue of timestamped counts for that vertex
	timestampedPodCounts map[int]*sharedqueue.OverflowQueue[*TimestampedCounts]
	// userSpecifiedLookBackSeconds is a map between replicaIndex and the user-specified lookback seconds for that replica
	userSpecifiedLookBackSeconds map[int]int64
	options                      *options
}

// PodReadCount is a struct to maintain count of messages read from each partition by a pod
type PodReadCount struct {
	// pod name
	name string
	// represents the count of messages read by the corresponding pod
	readCount float64
}

func (p *PodReadCount) Name() string {
	return p.name
}

func (p *PodReadCount) ReadCount() float64 {
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
		log:                          logging.FromContext(ctx).Named("Rater"),
		timestampedPodCounts:         make(map[int]*sharedqueue.OverflowQueue[*TimestampedCounts]),
		userSpecifiedLookBackSeconds: make(map[int]int64),
		options:                      defaultOptions(),
	}

	rater.podTracker = NewPodTracker(ctx, mv)
	for i := 0; i < mv.GetReplicas(); i++ {
		// maintain the total counts of the last 30 minutes(1800 seconds) since we support 1m, 5m, 15m lookback seconds.
		rater.timestampedPodCounts[i] = sharedqueue.New[*TimestampedCounts](int(1800 / CountWindow.Seconds()))
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
func (r *Rater) getPodReadCounts(replica int, podName string) *PodReadCount {
	// TODO(MonoVertex) : update the
	readTotalMetricName := "XXXXXXX"
	headlessServiceName := r.monoVertex.GetHeadlessServiceName()
	// scrape the read total metric from pod metric port
	// example for 0th pod: https://simple-mono-vertex-mv-0.simple-mono-vertex-mv-headless.default.svc:2469/metrics
	url := fmt.Sprintf("https://%s.%s.%s.svc:%v/metrics", podName, headlessServiceName, r.monoVertex.Namespace, v1alpha1.MonoVertexMetricsPort)
	resp, err := r.httpClient.Get(url)
	if err != nil {
		r.log.Errorf("[MonoVertex name %s, pod name %s]: failed reading the metrics endpoint, %v", r.monoVertex.Name, podName, err.Error())
		return nil
	}
	defer resp.Body.Close()

	textParser := expfmt.TextParser{}
	result, err := textParser.TextToMetricFamilies(resp.Body)
	if err != nil {
		r.log.Errorf("[MonoVertex name %s, pod name %s]:  failed parsing to prometheus metric families, %v", r.monoVertex.Name, podName, err.Error())
		return nil
	}

	// TODO(MonoVertex): Check the logic here
	if value, ok := result[readTotalMetricName]; ok && value != nil && len(value.GetMetric()) > 0 {
		metricsList := value.GetMetric()
		var partitionReadCount float64
		for _, ele := range metricsList {
			replicaIndex := -1
			for _, label := range ele.Label {
				if label.GetName() == metrics.LabelMonoVertexReplicaIndex {
					replicaIndex, _ = strconv.Atoi(label.GetValue())
					break
				}
			}
			if replicaIndex == -1 {
				r.log.Warnf("[MonoVertex name %s, pod name %s]: Replica is not found for metric %s", r.monoVertex.Name, podName, readTotalMetricName)
			} else {
				partitionReadCount = ele.Counter.GetValue()
			}
		}
		podReadCount := &PodReadCount{podName, partitionReadCount}
		return podReadCount
	} else {
		r.log.Errorf("[MonoVertex name  %s, pod name %s]: failed getting the read total metric, the metric is not available.", r.monoVertex.Name, podName)
		return nil
	}
}

// GetRates returns the processing rates of the vertex partition in the format of lookback second to rate mappings
func (r *Rater) GetRates(replicaIdx int) map[string]*wrapperspb.DoubleValue {
	r.log.Debugf("Current timestampedPodCounts for MonoVertex %s Replica %d is: %v", r.monoVertex.Name, replicaIdx, r.timestampedPodCounts[replicaIdx])
	var result = make(map[string]*wrapperspb.DoubleValue)
	// calculate rates for each lookback seconds
	for n, i := range r.buildLookbackSecondsMap(replicaIdx) {
		r := CalculateRate(r.timestampedPodCounts[replicaIdx], i)
		result[n] = wrapperspb.Double(r)
	}
	r.log.Debugf("Got rates for MonoVertex %s, replica %d: %v", r.monoVertex.Name, replicaIdx, result)
	return result
}

func (r *Rater) buildLookbackSecondsMap(replicaIndex int) map[string]int64 {
	lookbackSecondsMap := map[string]int64{"default": r.userSpecifiedLookBackSeconds[replicaIndex]}
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
