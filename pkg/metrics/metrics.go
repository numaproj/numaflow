package metrics

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedqueue "github.com/numaproj/numaflow/pkg/shared/queue"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	processingRate = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vertex_processing_rate",
		Help: "Message processing rate in the last period of seconds, tps. It represents the rate of a vertex instead of a pod.",
	}, []string{"vertex", "pipeline", "period"})

	pending = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "vertex_pending_messages",
		Help: "Average pending messages in the last period of seconds. It is the pending messages of a vertex, not a pod.",
	}, []string{"pipeline", "vertex", "period"})

	// Always expose metrics of following lookback seconds (1m, 5m, 15m)
	fixedLookbackSeconds = map[string]int64{"1m": 60, "5m": 300, "15m": 900}
)

// timestampedPending is a helper struct to wrap a pending number and timestamp pair
type timestampedPending struct {
	pending int64
	// timestamp in seconds
	timestamp int64
}

type metricsServer struct {
	vertex    *dfv1.Vertex
	rater     isb.Ratable
	lagReader isb.LagReader
	// lookbackSeconds is the look back seconds for pending and rate calculation used for auto scaling
	lookbackSeconds     int64
	lagCheckingInterval time.Duration
	refreshInterval     time.Duration
	// pendingInfo stores a list of pending/timestamp(seconds) information
	pendingInfo *sharedqueue.OverflowQueue[timestampedPending]
}

type Option func(*metricsServer)

func WithRater(r isb.Ratable) Option {
	return func(m *metricsServer) {
		m.rater = r
	}
}

func WithLagReader(r isb.LagReader) Option {
	return func(m *metricsServer) {
		m.lagReader = r
	}
}

func WithRefreshInterval(d time.Duration) Option {
	return func(m *metricsServer) {
		m.refreshInterval = d
	}
}

func WithLookbackSeconds(seconds int64) Option {
	return func(m *metricsServer) {
		m.lookbackSeconds = seconds
	}
}

// NewMetricsServer returns a Prometheus metrics server instance, which can be used to start an HTTPS service to expose Prometheus metrics.
func NewMetricsServer(vertex *dfv1.Vertex, opts ...Option) *metricsServer {
	m := new(metricsServer)
	m.vertex = vertex
	m.refreshInterval = 5 * time.Second     // Default refersh interval
	m.lagCheckingInterval = 3 * time.Second // Default lag checking interval
	m.lookbackSeconds = 180                 // Default
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	if m.lagReader != nil {
		m.pendingInfo = sharedqueue.New[timestampedPending](1800)
	}
	return m
}

// Enqueue pending pending information
func (ms *metricsServer) buildupPendingInfo(ctx context.Context) {
	if ms.lagReader == nil {
		return
	}
	log := logging.FromContext(ctx)
	ticker := time.NewTicker(ms.lagCheckingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if pending, err := ms.lagReader.Pending(ctx); err != nil {
				log.Errorw("failed to get pending messages", zap.Error(err))
			} else {
				if pending != isb.PendingNotAvailable {
					ts := timestampedPending{pending: pending, timestamp: time.Now().Unix()}
					ms.pendingInfo.Append(ts)
				}
			}
		}
	}
}

// Expose pending and rate metrics
func (ms *metricsServer) exposePendingAndRate(ctx context.Context) {
	if ms.lagReader == nil && ms.rater == nil {
		return
	}
	log := logging.FromContext(ctx)
	lookbackSecondsMap := map[string]int64{"default": ms.lookbackSeconds}
	for k, v := range fixedLookbackSeconds {
		lookbackSecondsMap[k] = v
	}
	ticker := time.NewTicker(ms.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if ms.rater != nil {
				for n, i := range lookbackSecondsMap {
					if r, err := ms.rater.Rate(ctx, i); err != nil {
						log.Errorw("failed to get processing rate in the past seconds", zap.Int64("seconds", i), zap.Error(err))
					} else {
						if r != isb.RateNotAvailable {
							processingRate.WithLabelValues(ms.vertex.Spec.PipelineName, ms.vertex.Spec.Name, n).Set(r)
						}
					}
				}
			}
			if ms.lagReader != nil {
				for n, i := range lookbackSecondsMap {
					if p := ms.calculatePending(i); p != isb.PendingNotAvailable {
						pending.WithLabelValues(ms.vertex.Spec.PipelineName, ms.vertex.Spec.Name, n).Set(float64(p))
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// Calculate the avg pending of last seconds
func (ms *metricsServer) calculatePending(seconds int64) int64 {
	result := isb.PendingNotAvailable
	items := ms.pendingInfo.Items()
	total := int64(0)
	num := int64(0)
	now := time.Now().Unix()
	for i := len(items) - 1; i >= 0; i-- {
		if now-items[i].timestamp < seconds {
			total += items[i].pending
			num++
		} else {
			break
		}
	}
	if num > 0 {
		result = total / num
	}
	return result
}

// Start function starts the HTTPS service to expose metrics, it returns a shutdown function and an error if any
func (ms *metricsServer) Start(ctx context.Context) (func(ctx context.Context) error, error) {
	log := logging.FromContext(ctx)
	log.Info("Generating self-signed certificate")
	cer, err := sharedtls.GenerateX509KeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate cert: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(204)
	})
	debugEnabled := os.Getenv(dfv1.EnvDebug)
	if debugEnabled == "true" {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	} else {
		log.Info("Not enabling pprof debug endpoints")
	}

	httpServer := &http.Server{
		Addr:      fmt.Sprintf(":%d", dfv1.VertexMetricsPort),
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{*cer}, MinVersion: tls.VersionTLS12},
	}
	// Build up pending information
	go ms.buildupPendingInfo(ctx)
	// Expose pending and rate metrics
	go ms.exposePendingAndRate(ctx)
	go func() {
		log.Info("Starting metrics HTTPS server")
		if err := httpServer.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			log.Fatalw("Failed to listen-and-server on HTTPS", zap.Error(err))
		}
		log.Info("Metrics server shutdown")
	}()
	return httpServer.Shutdown, nil
}
