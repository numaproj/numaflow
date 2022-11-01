package http

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	metricspkg "github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
)

type httpSource struct {
	name         string
	pipelineName string
	ready        bool
	readTimeout  time.Duration
	bufferSize   int
	messages     chan *isb.ReadMessage
	logger       *zap.SugaredLogger

	forwarder *forward.InterStepDataForward
	// source watermark publisher
	sourcePublishWM publish.Publisher
	// context cancel function
	cancelfn context.CancelFunc
	shutdown func(context.Context) error
}

type Option func(*httpSource) error

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *httpSource) error {
		o.logger = l
		return nil
	}
}

// WithReadTimeout is used to set the read timeout for the from buffer
func WithReadTimeout(t time.Duration) Option {
	return func(o *httpSource) error {
		o.readTimeout = t
		return nil
	}
}

func WithBufferSize(s int) Option {
	return func(o *httpSource) error {
		o.bufferSize = s
		return nil
	}
}

func New(vertexInstance *dfv1.VertexInstance, writers []isb.BufferWriter, fetchWM fetch.Fetcher, publishWM map[string]publish.Publisher, publishWMStores store.WatermarkStorer, opts ...Option) (*httpSource, error) {
	h := &httpSource{
		name:         vertexInstance.Vertex.Spec.Name,
		pipelineName: vertexInstance.Vertex.Spec.PipelineName,
		ready:        false,
		bufferSize:   1000,            // default size
		readTimeout:  1 * time.Second, // default timeout
	}
	for _, o := range opts {
		operr := o(h)
		if operr != nil {
			return nil, operr
		}
	}
	if h.logger == nil {
		h.logger = logging.NewLogger()
	}
	h.messages = make(chan *isb.ReadMessage, h.bufferSize)

	auth := ""
	if x := vertexInstance.Vertex.Spec.Source.HTTP.Auth; x != nil && x.Token != nil {
		if s, err := sharedutil.GetSecretFromVolume(x.Token); err != nil {
			return nil, fmt.Errorf("failed to get auth token, %w", err)
		} else {
			auth = string(s)
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !h.ready {
			w.WriteHeader(503)
			_, _ = w.Write([]byte("503 not ready\n"))
			return
		}
		w.WriteHeader(204)
	})
	mux.HandleFunc("/vertices/"+vertexInstance.Vertex.Spec.Name, func(w http.ResponseWriter, r *http.Request) {
		if auth != "" && r.Header.Get("Authorization") != "Bearer "+auth {
			w.WriteHeader(403)
			_, _ = w.Write([]byte("403 forbidden\n"))
			return
		}
		if !h.ready {
			w.WriteHeader(503)
			_, _ = w.Write([]byte("503 not ready\n"))
			return
		}
		msg, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		id := r.Header.Get(dfv1.KeyMetaID)
		if id == "" {
			id = uuid.New().String()
		}
		eventTime := time.Now()
		if x := r.Header.Get(dfv1.KeyMetaEventTime); x != "" {
			i, err := strconv.ParseInt(x, 10, 64)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(err.Error()))
				return
			}
			eventTime = time.UnixMilli(i)
		}
		m := &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					PaneInfo: isb.PaneInfo{EventTime: eventTime},
					ID:       id,
				},
				Body: isb.Body{
					Payload: msg,
				},
			},
			ReadOffset: isb.SimpleStringOffset(func() string { return id }),
		}
		h.messages <- m
		w.WriteHeader(http.StatusNoContent)
	})
	cer, err := sharedtls.GenerateX509KeyPair()
	if err != nil {
		return nil, fmt.Errorf("failed to generate cert: %w", err)
	}
	server := &http.Server{
		Addr:      fmt.Sprintf(":%d", dfv1.VertexHTTPSPort),
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{*cer}, MinVersion: tls.VersionTLS12},
	}
	go func() {
		h.logger.Info("Starting http source server")
		if err := server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
			h.logger.Fatalw("Failed to listen-and-server on http source server", zap.Error(err))
		}
		h.logger.Info("Shutdown http source server")
	}()
	h.shutdown = server.Shutdown

	destinations := make(map[string]isb.BufferWriter, len(writers))
	for _, w := range writers {
		destinations[w.GetName()] = w
	}

	forwardOpts := []forward.Option{forward.WithVertexType(dfv1.VertexTypeSource), forward.WithLogger(h.logger)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	forwarder, err := forward.NewInterStepDataForward(vertexInstance.Vertex, h, destinations, forward.All, applier.Terminal, fetchWM, publishWM, forwardOpts...)
	if err != nil {
		h.logger.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	h.forwarder = forwarder
	ctx, cancel := context.WithCancel(context.Background())
	h.cancelfn = cancel
	entityName := fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
	processorEntity := processor.NewProcessorEntity(entityName)
	h.sourcePublishWM = publish.NewPublish(ctx, processorEntity, publishWMStores, publish.IsSource(), publish.WithDelay(sharedutil.GetWatermarkMaxDelay()))
	return h, nil
}

func (h *httpSource) GetName() string {
	return h.name
}

func (h *httpSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	msgs := []*isb.ReadMessage{}
	var latest time.Time
	timeout := time.After(h.readTimeout)
loop:
	for i := int64(0); i < count; i++ {
		select {
		case m := <-h.messages:
			if latest.IsZero() || m.EventTime.After(latest) {
				latest = m.EventTime
			}
			msgs = append(msgs, m)
			httpSourceReadCount.With(map[string]string{metricspkg.LabelVertex: h.name, metricspkg.LabelPipeline: h.pipelineName}).Inc()
		case <-timeout:
			h.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", h.readTimeout), zap.Int("read", len(msgs)))
			break loop
		}
	}
	h.logger.Debugf("Read %d messages.", len(msgs))
	if len(msgs) > 0 && !latest.IsZero() {
		h.sourcePublishWM.PublishWatermark(processor.Watermark(latest), msgs[len(msgs)-1].ReadOffset)
	}
	return msgs, nil
}

func (h *httpSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	return make([]error, len(offsets))
}

func (h *httpSource) Close() error {
	h.logger.Info("Shutting down http source server...")
	h.cancelfn()
	close(h.messages)
	if err := h.shutdown(context.Background()); err != nil {
		return err
	}
	h.logger.Info("HTTP source server shutdown")
	return nil
}

func (h *httpSource) Stop() {
	h.logger.Info("Stopping http reader...")
	h.ready = false
	h.forwarder.Stop()
}

func (h *httpSource) ForceStop() {
	h.Stop()
}

func (h *httpSource) Start() <-chan struct{} {
	defer func() { h.ready = true }()
	return h.forwarder.Start()
}
