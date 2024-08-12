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

// Rater is a struct that maintains information about the processing rate of the MonoVertex.
// It monitors the number of processed messages for each pod in a MonoVertex and calculates the rate.
type Rater struct {
	monoVertex *v1alpha1.MonoVertex
	httpClient metricsHttpClient
	log        *zap.SugaredLogger
	podTracker *PodTracker
	// timestampedPodCounts is a queue of timestamped counts for the vertex
	timestampedPodCounts *sharedqueue.OverflowQueue[*TimestampedCounts]
	// userSpecifiedLookBackSeconds is the user-specified lookback seconds for that MonoVertex
	userSpecifiedLookBackSeconds int64
	options                      *options
}

// PodReadCount is a struct to maintain count of messages read by a pod
type PodReadCount struct {
	// pod name
	name string
	// represents the count of messages read by the pod
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
		log:     logging.FromContext(ctx).Named("Rater"),
		options: defaultOptions(),
	}

	rater.podTracker = NewPodTracker(ctx, mv)
	// maintain the total counts of the last 30 minutes(1800 seconds) since we support 1m, 5m, 15m lookback seconds.
	rater.timestampedPodCounts = sharedqueue.New[*TimestampedCounts](int(1800 / CountWindow.Seconds()))
	rater.userSpecifiedLookBackSeconds = int64(mv.Spec.Scale.GetLookbackSeconds())

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
		podReadCount = r.getPodReadCounts(pInfo.podName)
		if podReadCount == nil {
			log.Debugf("Failed retrieving total podReadCounts for pod %s", pInfo.podName)
		}
	} else {
		log.Debugf("Pod %s does not exist, updating it with nil...", pInfo.podName)
		podReadCount = nil
	}
	now := time.Now().Add(CountWindow).Truncate(CountWindow).Unix()
	UpdateCount(r.timestampedPodCounts, now, podReadCount)
	return nil
}

// getPodReadCounts returns the total number of messages read by the pod
// since a pod can read from multiple partitions, we will return a map of partition to read count.
func (r *Rater) getPodReadCounts(podName string) *PodReadCount {
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
		// Each pod should be emitting only one metric with this name, so we should be able to take the first value
		// from the results safely.
		podReadCount := &PodReadCount{podName, metricsList[0].Counter.GetValue()}
		return podReadCount
	} else {
		r.log.Errorf("[MonoVertex name  %s, pod name %s]: failed getting the read total metric, the metric is not available.", r.monoVertex.Name, podName)
		return nil
	}
}

// GetRates returns the processing rates of the vertex partition in the format of lookback second to rate mappings
func (r *Rater) GetRates() map[string]*wrapperspb.DoubleValue {
	r.log.Debugf("Current timestampedPodCounts for MonoVertex %s is: %v", r.monoVertex.Name, r.timestampedPodCounts)
	var result = make(map[string]*wrapperspb.DoubleValue)
	// calculate rates for each lookback seconds
	for n, i := range r.buildLookbackSecondsMap() {
		rate := CalculateRate(r.timestampedPodCounts, i)
		result[n] = wrapperspb.Double(rate)
	}
	r.log.Debugf("Got rates for MonoVertex %s: %v", r.monoVertex.Name, result)
	return result
}

func (r *Rater) buildLookbackSecondsMap() map[string]int64 {
	lookbackSecondsMap := map[string]int64{"default": r.userSpecifiedLookBackSeconds}
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
