package http

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/udf/applier"
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
	shutdown  func(context.Context) error
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

func New(vertex *dfv1.Vertex, writers []isb.BufferWriter, opts ...Option) (*httpSource, error) {
	h := &httpSource{
		name:         vertex.Spec.Name,
		pipelineName: vertex.Spec.PipelineName,
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
	if x := vertex.Spec.Source.HTTP.Auth; x != nil && x.Token != nil {
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
	mux.HandleFunc("/vertices/"+vertex.Spec.Name, func(w http.ResponseWriter, r *http.Request) {
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
		msg, err := ioutil.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		id := r.Header.Get(dfv1.KeyMetaID)
		if id == "" {
			id = uuid.New().String()
		}
		m := &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					PaneInfo: isb.PaneInfo{EventTime: time.Now()},
					ID:       id,
				},
				Body: isb.Body{
					Payload: msg,
				},
			},
			ReadOffset: isb.SimpleOffset(func() string { return id }),
		}
		h.messages <- m
		w.WriteHeader(204)
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

	forwardOpts := []forward.Option{forward.WithLogger(h.logger)}
	if x := vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	forwarder, err := forward.NewInterStepDataForward(vertex, h, destinations, forward.All, applier.Terminal, nil, forwardOpts...)
	if err != nil {
		h.logger.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	h.forwarder = forwarder
	return h, nil
}

func (h *httpSource) GetName() string {
	return h.name
}

func (h *httpSource) Pending(_ context.Context) (int64, error) {
	return isb.PendingNotAvailable, nil
}

func (h *httpSource) Read(ctx context.Context, count int64) ([]*isb.ReadMessage, error) {
	msgs := []*isb.ReadMessage{}
	timeout := time.After(h.readTimeout)
	for i := int64(0); i < count; i++ {
		select {
		case m := <-h.messages:
			msgs = append(msgs, m)
		case <-timeout:
			h.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", h.readTimeout), zap.Int("read", len(msgs)))
			return msgs, nil
		}
	}
	httpSourceReadCount.With(map[string]string{"vertex": h.name, "pipeline": h.pipelineName}).Inc()
	h.logger.Debugf("Read %d messages.", len(msgs))
	return msgs, nil
}

func (h *httpSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	return make([]error, len(offsets))
}

func (h *httpSource) Close() error {
	h.logger.Info("Shutting down http source server...")
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
