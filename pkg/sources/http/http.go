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

package http

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	sourceforward "github.com/numaproj/numaflow/pkg/sources/forward"
	"github.com/numaproj/numaflow/pkg/sources/forward/applier"
	"github.com/numaproj/numaflow/pkg/sources/sourcer"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type httpSource struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	ready         bool
	readTimeout   time.Duration
	bufferSize    int
	messages      chan *isb.ReadMessage
	logger        *zap.SugaredLogger
	forwarder     *sourceforward.DataForward
	cancelFunc    context.CancelFunc // context cancel function
	shutdown      func(context.Context) error
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

func New(
	vertexInstance *dfv1.VertexInstance,
	writers map[string][]isb.BufferWriter,
	fsd forwarder.ToWhichStepDecider,
	transformerApplier applier.SourceTransformApplier,
	fetchWM fetch.SourceFetcher,
	toVertexPublisherStores map[string]store.WatermarkStore,
	publishWMStores store.WatermarkStore,
	idleManager wmb.IdleManager,
	opts ...Option) (sourcer.Sourcer, error) {

	h := &httpSource{
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		ready:         false,
		bufferSize:    1000,            // default size
		readTimeout:   1 * time.Second, // default timeout
	}

	for _, o := range opts {
		if err := o(h); err != nil {
			return nil, err
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
			auth = s
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if !h.ready {
			http.Error(w, "http source not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/vertices/"+vertexInstance.Vertex.Spec.Name, func(w http.ResponseWriter, r *http.Request) {
		if auth != "" && r.Header.Get("Authorization") != "Bearer "+auth {
			http.Error(w, "request not authorized", http.StatusForbidden)
			return
		}
		if !h.ready {
			http.Error(w, "http source not ready", http.StatusServiceUnavailable)
			return
		}
		msg, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
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
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			eventTime = time.UnixMilli(i)
		}
		m := &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{EventTime: eventTime},
					ID:          id,
				},
				Body: isb.Body{
					Payload: msg,
				},
			},
			ReadOffset: isb.NewSimpleStringPartitionOffset(id, vertexInstance.Replica),
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
		if err := server.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.logger.Fatalw("Failed to listen-and-server on http source server", zap.Error(err))
		}
		h.logger.Info("Shutdown http source server")
	}()
	h.shutdown = server.Shutdown

	forwardOpts := []sourceforward.Option{sourceforward.WithLogger(h.logger)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, sourceforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancelFunc = cancel

	// create a source watermark publisher
	sourceWmPublisher := publish.NewSourcePublish(ctx, vertexInstance.Vertex.Spec.PipelineName, vertexInstance.Vertex.Spec.Name,
		publishWMStores, publish.WithDelay(vertexInstance.Vertex.Spec.Watermark.GetMaxDelay()), publish.WithDefaultPartitionIdx(vertexInstance.Replica))
	h.forwarder, err = sourceforward.NewDataForward(vertexInstance, h, writers, fsd, transformerApplier, fetchWM, sourceWmPublisher, toVertexPublisherStores, idleManager, forwardOpts...)
	if err != nil {
		h.logger.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}

	return h, nil
}

// GetName returns the name of the source.
func (h *httpSource) GetName() string {
	return h.vertexName
}

// Partitions returns the partitions for the source.
func (h *httpSource) Partitions() []int32 {
	return []int32{h.vertexReplica}
}

func (h *httpSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	var msgs []*isb.ReadMessage
	timeout := time.After(h.readTimeout)
loop:
	for i := int64(0); i < count; i++ {
		select {
		case m := <-h.messages:
			httpSourceReadCount.With(map[string]string{metrics.LabelVertex: h.vertexName, metrics.LabelPipeline: h.pipelineName}).Inc()
			msgs = append(msgs, m)
		case <-timeout:
			h.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", h.readTimeout), zap.Int("read", len(msgs)))
			break loop
		}
	}
	h.logger.Debugf("Read %d messages.", len(msgs))
	return msgs, nil
}

func (h *httpSource) Pending(_ context.Context) (int64, error) {
	return isb.PendingNotAvailable, nil
}

func (h *httpSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	return make([]error, len(offsets))
}

func (h *httpSource) Close() error {
	h.logger.Info("Shutting down http source server...")
	h.cancelFunc()
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
