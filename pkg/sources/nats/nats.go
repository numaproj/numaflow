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

package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	natslib "github.com/nats-io/nats.go"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/processor"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type natsSource struct {
	name         string
	pipelineName string
	logger       *zap.SugaredLogger

	natsConn *natslib.Conn
	sub      *natslib.Subscription

	bufferSize  int
	messages    chan *isb.ReadMessage
	readTimeout time.Duration

	cancelfn  context.CancelFunc
	forwarder *forward.InterStepDataForward
	// source watermark publisher
	sourcePublishWM publish.Publisher
}

func New(
	vertexInstance *dfv1.VertexInstance,
	writers []isb.BufferWriter,
	fsd forward.ToWhichStepDecider,
	mapApplier applier.MapApplier,
	fetchWM fetch.Fetcher,
	publishWM map[string]publish.Publisher,
	publishWMStores store.WatermarkStorer,
	opts ...Option) (*natsSource, error) {

	n := &natsSource{
		name:         vertexInstance.Vertex.Spec.Name,
		pipelineName: vertexInstance.Vertex.Spec.PipelineName,
		bufferSize:   1000,            // default size
		readTimeout:  1 * time.Second, // default timeout
	}
	for _, o := range opts {
		operr := o(n)
		if operr != nil {
			return nil, operr
		}
	}
	if n.logger == nil {
		n.logger = logging.NewLogger()
	}
	n.messages = make(chan *isb.ReadMessage, n.bufferSize)

	destinations := make(map[string]isb.BufferWriter, len(writers))
	for _, w := range writers {
		destinations[w.GetName()] = w
	}
	forwardOpts := []forward.Option{forward.WithVertexType(dfv1.VertexTypeSource), forward.WithLogger(n.logger), forward.WithSourceWatermarkPublisher(n)}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	forwarder, err := forward.NewInterStepDataForward(vertexInstance.Vertex, n, destinations, fsd, mapApplier, fetchWM, publishWM, forwardOpts...)
	if err != nil {
		n.logger.Errorw("Error instantiating the forwarder", zap.Error(err))
		return nil, err
	}
	n.forwarder = forwarder
	ctx, cancel := context.WithCancel(context.Background())
	n.cancelfn = cancel
	entityName := fmt.Sprintf("%s-%d", vertexInstance.Vertex.Name, vertexInstance.Replica)
	processorEntity := processor.NewProcessorEntity(entityName)
	n.sourcePublishWM = publish.NewPublish(ctx, processorEntity, publishWMStores, publish.IsSource(), publish.WithDelay(vertexInstance.Vertex.Spec.Watermark.GetMaxDelay()))

	source := vertexInstance.Vertex.Spec.Source.Nats
	opt := []natslib.Option{
		natslib.MaxReconnects(-1),
		natslib.ReconnectWait(3 * time.Second),
		natslib.DisconnectHandler(func(c *natslib.Conn) {
			n.logger.Info("Nats disconnected")
		}),
		natslib.ReconnectHandler(func(c *natslib.Conn) {
			n.logger.Info("Nats reconnected")
		}),
	}
	if source.TLS != nil {
		if c, err := sharedutil.GetTLSConfig(source.TLS); err != nil {
			return nil, err
		} else {
			opt = append(opt, natslib.Secure(c))
		}
	}

	if source.Auth != nil {
		switch {
		case source.Auth.Basic != nil && source.Auth.Basic.User != nil && source.Auth.Basic.Password != nil:
			username, err := sharedutil.GetSecretFromVolume(source.Auth.Basic.User)
			if err != nil {
				return nil, fmt.Errorf("failed to get basic auth user, %w", err)
			}
			password, err := sharedutil.GetSecretFromVolume(source.Auth.Basic.Password)
			if err != nil {
				return nil, fmt.Errorf("failed to get basic auth password, %w", err)
			}
			opt = append(opt, natslib.UserInfo(username, password))
		case source.Auth.Token != nil:
			token, err := sharedutil.GetSecretFromVolume(source.Auth.Token)
			if err != nil {
				return nil, fmt.Errorf("failed to get auth token, %w", err)
			}
			opt = append(opt, natslib.Token(token))
		case source.Auth.NKey != nil:
			nkeyFile, err := sharedutil.GetSecretVolumePath(source.Auth.NKey)
			if err != nil {
				return nil, fmt.Errorf("failed to get configured nkey file, %w", err)
			}
			o, err := natslib.NkeyOptionFromSeed(nkeyFile)
			if err != nil {
				return nil, fmt.Errorf("failed to get NKey, %w", err)
			}
			opt = append(opt, o)
		}
	}

	n.logger.Info("Connecting to nats service...")
	if conn, err := natslib.Connect(source.URL, opt...); err != nil {
		return nil, fmt.Errorf("failed to connect to nats server, %w", err)
	} else {
		n.natsConn = conn
	}
	if sub, err := n.natsConn.QueueSubscribe(source.Subject, source.Queue, func(msg *natslib.Msg) {
		id := uuid.New().String()
		m := &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					// TODO: Be able to specify event time.
					MessageInfo: isb.MessageInfo{EventTime: time.Now()},
					ID:          id,
				},
				Body: isb.Body{
					Payload: msg.Data,
				},
			},
			// TODO: Be able to specify an ID for dedup?
			ReadOffset: isb.SimpleStringOffset(func() string { return id }),
		}
		n.messages <- m
	}); err != nil {
		n.natsConn.Close()
		return nil, fmt.Errorf("failed to QueueSubscribe nats messages, %w", err)
	} else {
		n.sub = sub
	}
	return n, nil
}

type Option func(*natsSource) error

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *natsSource) error {
		o.logger = l
		return nil
	}
}

// WithBufferSize sets the buffer size for storing the messages from nats
func WithBufferSize(s int) Option {
	return func(o *natsSource) error {
		o.bufferSize = s
		return nil
	}
}

// WithReadTimeout sets the read timeout
func WithReadTimeout(t time.Duration) Option {
	return func(o *natsSource) error {
		o.readTimeout = t
		return nil
	}
}

func (ns *natsSource) GetName() string {
	return ns.name
}

func (ns *natsSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	var msgs []*isb.ReadMessage
	timeout := time.After(ns.readTimeout)
loop:
	for i := int64(0); i < count; i++ {
		select {
		case m := <-ns.messages:
			natsSourceReadCount.With(map[string]string{metrics.LabelVertex: ns.name, metrics.LabelPipeline: ns.pipelineName}).Inc()
			msgs = append(msgs, m)
		case <-timeout:
			ns.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", ns.readTimeout), zap.Int("read", len(msgs)))
			break loop
		}
	}
	ns.logger.Debugf("Read %d messages.", len(msgs))
	return msgs, nil
}

func (ns *natsSource) PublishSourceWatermarks(msgs []*isb.ReadMessage) {
	var oldest time.Time
	for _, m := range msgs {
		if oldest.IsZero() || m.EventTime.Before(oldest) {
			oldest = m.EventTime
		}
	}
	if len(msgs) > 0 && !oldest.IsZero() {
		ns.sourcePublishWM.PublishWatermark(wmb.Watermark(oldest), nil) // Source publisher does not care about the offset
	}
}

func (ns *natsSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	return make([]error, len(offsets))
}

func (ns *natsSource) Close() error {
	ns.logger.Info("Shutting down nats source server...")
	ns.cancelfn()
	if err := ns.sourcePublishWM.Close(); err != nil {
		ns.logger.Errorw("Failed to close source vertex watermark publisher", zap.Error(err))
	}
	if err := ns.sub.Unsubscribe(); err != nil {
		ns.logger.Errorw("Failed to unsubscribe nats subscription", zap.Error(err))
	}
	ns.natsConn.Close()
	ns.logger.Info("Nats source server shutdown")
	return nil
}

func (ns *natsSource) Stop() {
	ns.logger.Info("Stopping nats reader...")
	ns.forwarder.Stop()
}

func (ns *natsSource) ForceStop() {
	ns.Stop()
}

func (ns *natsSource) Start() <-chan struct{} {
	return ns.forwarder.Start()
}
