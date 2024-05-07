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

package jetstream

import (
	"context"
	"fmt"
	"time"

	natslib "github.com/nats-io/nats.go"
	jetstreamlib "github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/sources/sourcer"
)

type jsSource struct {
	vertexName     string
	pipelineName   string
	vertexReplica  int32
	logger         *zap.SugaredLogger
	natsConn       *natslib.Conn
	consumer       jetstreamlib.Consumer
	consumerHandle jetstreamlib.ConsumeContext
	bufferSize     int
	messages       chan *isb.ReadMessage
	readTimeout    time.Duration
}

var _ sourcer.SourceReader = (*jsSource)(nil)

func New(ctx context.Context, vertexInstance *dfv1.VertexInstance, opts ...Option) (sourcer.SourceReader, error) {
	n := &jsSource{
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		bufferSize:    1000,            // default size
		readTimeout:   1 * time.Second, // default timeout
	}
	for _, o := range opts {
		if err := o(n); err != nil {
			return nil, err
		}
	}
	if n.logger == nil {
		n.logger = logging.NewLogger()
	}
	n.messages = make(chan *isb.ReadMessage, n.bufferSize)

	source := vertexInstance.Vertex.Spec.Source.JetStream
	opt := []natslib.Option{
		natslib.MaxReconnects(-1),
		natslib.ReconnectWait(3 * time.Second),
		natslib.DisconnectErrHandler(func(c *natslib.Conn, err error) {
			n.logger.Errorw("Nats disconnected", zap.Error(err))
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
	conn, err := natslib.Connect(source.URL, opt...)
	if err != nil {
		return nil, fmt.Errorf("connecting to NATS server %q: %w", source.URL, err)
	}
	n.natsConn = conn

	stream, err := jetstreamlib.New(conn)
	if err != nil {
		n.natsConn.Close()
		return nil, fmt.Errorf("creating jetstream instance for the NATS server %q: %w", source.URL, err)
	}

	consumerName := fmt.Sprintf("numaflow-%s-%s-%s", n.pipelineName, n.vertexName, source.Stream)
	consumer, err := stream.CreateOrUpdateConsumer(ctx, source.Stream, jetstreamlib.ConsumerConfig{
		Durable:       consumerName,
		Description:   "Numaflow JetStream consumer",
		DeliverPolicy: jetstreamlib.DeliverAllPolicy,
		AckPolicy:     jetstreamlib.AckExplicitPolicy,
	})
	n.consumer = consumer

	if err != nil {
		n.natsConn.Close()
		return nil, fmt.Errorf("creating jetstream consumer for stream %q: %w", source.Stream, err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstreamlib.Msg) {
		metadata, err := msg.Metadata()
		if err != nil {
			n.logger.Errorw("Getting metadata for the message", zap.Error(err))
			return
		}
		m := &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{EventTime: metadata.Timestamp},
				},
				Body: isb.Body{
					Payload: msg.Data(),
				},
			},
			ReadOffset: newOffset(msg, metadata, vertexInstance.Replica),
		}
		n.messages <- m
	})

	if err != nil {
		n.natsConn.Close()
		return nil, fmt.Errorf("registering jetstream consumer handler: %w", err)
	}
	n.consumerHandle = consumeCtx

	return n, nil
}

type Option func(*jsSource) error

// WithLogger is used to return logger information
func WithLogger(l *zap.SugaredLogger) Option {
	return func(o *jsSource) error {
		o.logger = l
		return nil
	}
}

// WithBufferSize sets the buffer size for storing the messages from nats
func WithBufferSize(s int) Option {
	return func(o *jsSource) error {
		o.bufferSize = s
		return nil
	}
}

// WithReadTimeout sets the read timeout
func WithReadTimeout(t time.Duration) Option {
	return func(o *jsSource) error {
		o.readTimeout = t
		return nil
	}
}

func (ns *jsSource) GetName() string {
	return ns.vertexName
}

// Partitions returns the partitions associated with this source.
func (ns *jsSource) Partitions(context.Context) []int32 {
	return []int32{ns.vertexReplica}
}

func (ns *jsSource) Read(ctx context.Context, count int64) ([]*isb.ReadMessage, error) {
	var msgs []*isb.ReadMessage
	timeout := time.After(ns.readTimeout)
loop:
	for i := int64(0); i < count; i++ {
		select {
		case m := <-ns.messages:
			jetstreamSourceReadCount.With(map[string]string{metrics.LabelVertex: ns.vertexName, metrics.LabelPipeline: ns.pipelineName}).Inc()
			msgs = append(msgs, m)
		case <-timeout:
			ns.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", ns.readTimeout), zap.Int("read", len(msgs)))
			break loop
		case <-ctx.Done():
			ns.logger.Debugw("Context cancelled.", zap.Error(ctx.Err()))
		}
	}
	ns.logger.Debugf("Read %d messages.", len(msgs))
	return msgs, nil
}

func (ns *jsSource) Pending(ctx context.Context) (int64, error) {
	info, err := ns.consumer.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting consumer info: %w", err)
	}
	return int64(info.NumPending), nil
}

func (ns *jsSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	var errs []error
	for _, o := range offsets {
		if err := o.AckIt(); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (ns *jsSource) Close() error {
	ns.logger.Info("Shutting down Jetstream source server...")
	ns.consumerHandle.Drain()
	ns.natsConn.Close()
	ns.logger.Info("Jetstream source server shutdown")
	return nil
}
