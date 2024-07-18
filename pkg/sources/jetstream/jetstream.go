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
	"errors"
	"fmt"
	"os"
	"sync/atomic"
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
	// latestErr stores the latest error that occurred when consuming messages from Jetstream
	// This is used to return error to the caller of the `Read` method. This value is reset in the consume handler.
	// If the last consume operation was successful, we can ignore previous errors.
	latestErr      atomic.Value
	servingEnabled bool
	jsSpec         *dfv1.JetStreamSource
}

var _ sourcer.SourceReader = (*jsSource)(nil)

var errNil = errors.New("Nil")

// New creates a Jetstream source reader.
func New(ctx context.Context, vertexInstance *dfv1.VertexInstance, opts ...Option) (sourcer.SourceReader, error) {
	var latestErr atomic.Value
	latestErr.Store(errNil)
	n := &jsSource{
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		bufferSize:    1000,            // default size
		readTimeout:   1 * time.Second, // default timeout
		latestErr:     latestErr,
		logger:        logging.FromContext(ctx),
		jsSpec:        vertexInstance.Vertex.Spec.Source.JetStream,
	}

	for _, o := range opts {
		if err := o(n); err != nil {
			return nil, err
		}
	}

	n.messages = make(chan *isb.ReadMessage, n.bufferSize)

	// Get the NATS options based on the serving enabled flag
	nOpts, err := n.getNatsOptions()
	if err != nil {
		return nil, err
	}

	// Connect to the NATS server
	if err := n.connectToNatsService(nOpts); err != nil {
		return nil, err
	}

	// Initialize the consumer
	if err := n.initializeConsumer(ctx); err != nil {
		return nil, err
	}

	// Start a routine to stop the consumer when the context is cancelled
	n.startRoutineToStopConsumer(ctx)

	return n, nil
}

// getNatsOptions returns the NATS options based on the serving enabled flag.
func (ns *jsSource) getNatsOptions() ([]natslib.Option, error) {
	opt := []natslib.Option{
		natslib.MaxReconnects(-1),
		natslib.ReconnectWait(3 * time.Second),
		natslib.DisconnectErrHandler(func(c *natslib.Conn, err error) {
			ns.logger.Errorw("Nats disconnected", zap.Error(err))
		}),
		natslib.ReconnectHandler(func(c *natslib.Conn) {
			ns.logger.Info("Nats reconnected")
		}),
	}

	if ns.servingEnabled {
		sOpts, err := ns.getNatsOptionsForServing()
		if err != nil {
			return nil, err
		}
		opt = append(opt, sOpts...)
	} else {
		sOpts, err := ns.getNatsOptionsForSource()
		if err != nil {
			return nil, err
		}
		opt = append(opt, sOpts...)
	}

	return opt, nil
}

// connectToNatsService connects to the NATS server.
func (ns *jsSource) connectToNatsService(opts []natslib.Option) error {
	var url string
	var existing bool
	if ns.servingEnabled {
		url, existing = os.LookupEnv(dfv1.EnvISBSvcJetStreamURL)
		if !existing {
			return fmt.Errorf("environment variable %q not found", dfv1.EnvISBSvcJetStreamURL)
		}
	} else {
		url = ns.jsSpec.URL
	}
	conn, err := natslib.Connect(url, opts...)
	if err != nil {
		return fmt.Errorf("connecting to NATS server %q: %w", url, err)
	}
	ns.natsConn = conn

	return nil
}

// initializeConsumer creates a new consumer and registers the consumer handler.
func (ns *jsSource) initializeConsumer(ctx context.Context) error {
	consumer, err := ns.createOrUpdateConsumer(ctx)
	if err != nil {
		ns.natsConn.Close()
		return err
	}
	ns.consumer = consumer

	consumeCtx, err := ns.registerConsumerHandler(ctx)
	if err != nil {
		ns.natsConn.Close()
		return err
	}
	ns.consumerHandle = consumeCtx

	return nil
}

// startRoutineToStopConsumer starts a routine to stop the consumer when the context is cancelled.
func (ns *jsSource) startRoutineToStopConsumer(ctx context.Context) {
	go func() {
		<-ctx.Done()
		ns.logger.Warnw("Initiating shutdown of Jetstream source due to context cancellation", zap.Error(ctx.Err()))
		if err := ns.Close(); err != nil {
			ns.logger.Errorw("Stopping Jetstream source", zap.Error(err))
			return
		}
		ns.logger.Info("Stopped Jetstream source")
	}()
}

// getNatsOptionsForServing returns the NATS options for serving mode. It reads the credentials from the environment variables.
func (ns *jsSource) getNatsOptionsForServing() ([]natslib.Option, error) {
	var opt []natslib.Option
	user, existing := os.LookupEnv(dfv1.EnvISBSvcJetStreamUser)
	if !existing {
		return nil, fmt.Errorf("environment variable %q not found", dfv1.EnvISBSvcJetStreamUser)
	}
	password, existing := os.LookupEnv(dfv1.EnvISBSvcJetStreamPassword)
	if !existing {
		return nil, fmt.Errorf("environment variable %q not found", dfv1.EnvISBSvcJetStreamPassword)
	}
	opt = append(opt, natslib.UserInfo(user, password))
	return opt, nil
}

// getNatsOptionsForSource returns the NATS options for source mode. It reads the credentials from the JetStreamSource object.
func (ns *jsSource) getNatsOptionsForSource() ([]natslib.Option, error) {
	var opt []natslib.Option
	if ns.jsSpec.TLS != nil {
		if c, err := sharedutil.GetTLSConfig(ns.jsSpec.TLS); err != nil {
			return nil, err
		} else {
			opt = append(opt, natslib.Secure(c))
		}
	}

	if ns.jsSpec.Auth != nil {
		switch {
		case ns.jsSpec.Auth.Basic != nil && ns.jsSpec.Auth.Basic.User != nil && ns.jsSpec.Auth.Basic.Password != nil:
			username, err := sharedutil.GetSecretFromVolume(ns.jsSpec.Auth.Basic.User)
			if err != nil {
				return nil, fmt.Errorf("failed to get basic auth user, %w", err)
			}
			password, err := sharedutil.GetSecretFromVolume(ns.jsSpec.Auth.Basic.Password)
			if err != nil {
				return nil, fmt.Errorf("failed to get basic auth password, %w", err)
			}
			opt = append(opt, natslib.UserInfo(username, password))
		case ns.jsSpec.Auth.Token != nil:
			token, err := sharedutil.GetSecretFromVolume(ns.jsSpec.Auth.Token)
			if err != nil {
				return nil, fmt.Errorf("failed to get auth token, %w", err)
			}
			opt = append(opt, natslib.Token(token))
		case ns.jsSpec.Auth.NKey != nil:
			nkeyFile, err := sharedutil.GetSecretVolumePath(ns.jsSpec.Auth.NKey)
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
	return opt, nil
}

// createOrUpdateConsumer creates a new consumer or updates an existing consumer in the JetStream stream.
func (ns *jsSource) createOrUpdateConsumer(ctx context.Context) (jetstreamlib.Consumer, error) {
	var streamName string
	var existing bool

	if ns.servingEnabled {
		streamName, existing = os.LookupEnv(dfv1.EnvServingJetstreamStream)
		if !existing {
			return nil, fmt.Errorf("environment variable %q not found", dfv1.EnvServingJetstreamStream)
		}
	} else {
		streamName = ns.jsSpec.Stream
	}

	stream, err := jetstreamlib.New(ns.natsConn)
	if err != nil {
		ns.natsConn.Close()
		return nil, fmt.Errorf("creating jetstream instance for the NATS server: %w", err)
	}

	consumerName := fmt.Sprintf("numaflow-%s-%s-%s", ns.pipelineName, ns.vertexName, streamName)
	consumer, err := stream.CreateOrUpdateConsumer(ctx, streamName, jetstreamlib.ConsumerConfig{
		Durable:       consumerName,
		Description:   "Numaflow JetStream consumer",
		DeliverPolicy: jetstreamlib.DeliverAllPolicy,
		AckPolicy:     jetstreamlib.AckExplicitPolicy,
	})
	if err != nil {
		return nil, fmt.Errorf("creating jetstream consumer for stream %q: %w", streamName, err)
	}
	return consumer, nil
}

func (ns *jsSource) registerConsumerHandler(ctx context.Context) (jetstreamlib.ConsumeContext, error) {
	consumerInfo, err := ns.consumer.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching consumer info: %w", err)
	}
	ackWaitTime := consumerInfo.Config.AckWait
	// If ackWait is 3s, ticks every 2s.
	inProgressTickSeconds := int64(ackWaitTime.Seconds() * 2 / 3)
	if inProgressTickSeconds < 1 {
		inProgressTickSeconds = 1
	}
	inProgressTickDuration := time.Duration(inProgressTickSeconds * int64(time.Second))

	// Callback function to process a message
	msgHandler := func(msg jetstreamlib.Msg) {
		// Reset the error to nil since the recent consume call was successful
		ns.latestErr.Store(errNil)
		metadata, err := msg.Metadata()
		if err != nil {
			ns.logger.Errorw("Getting metadata for the message", zap.Error(err))
			return
		}

		// Headers are supposed to be map[string][]string. However isb.Message.Header.Headers is map[string]string.
		// So we only use the last header value in the slice.
		headers := make(map[string]string, len(msg.Headers()))
		for header, value := range msg.Headers() {
			headers[header] = value[len(value)-1]
		}

		readOffset := newOffset(msg, metadata.Sequence.Stream, inProgressTickDuration, ns.logger)

		m := &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					MessageInfo: isb.MessageInfo{EventTime: metadata.Timestamp},
					ID: isb.MessageID{
						VertexName: ns.vertexName,
						Offset:     readOffset.String(),
						Index:      readOffset.PartitionIdx(),
					},
					Headers: headers,
				},
				Body: isb.Body{
					Payload: msg.Data(),
				},
			},
			ReadOffset: readOffset,
		}
		ns.messages <- m
	}

	// callback for error handling
	consumeErrHandler := func(consumeCtx jetstreamlib.ConsumeContext, err error) {
		if errors.Is(err, jetstreamlib.ErrNoMessages) {
			return
		}
		if errors.Is(err, jetstreamlib.ErrNoHeartbeat) {
			ns.logger.Warnw("Ignoring the error", zap.Error(err))
			return
		}
		ns.latestErr.Store(err)
		ns.logger.Errorw("Consuming messages", zap.Error(err))
	}

	consumeCtx, err := ns.consumer.Consume(msgHandler, jetstreamlib.ConsumeErrHandler(consumeErrHandler))
	if err != nil {
		return nil, fmt.Errorf("registering jetstream consumer handler: %w", err)
	}
	return consumeCtx, nil
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

// WithServingEnabled sets the serving enabled flag
func WithServingEnabled() Option {
	return func(o *jsSource) error {
		o.servingEnabled = true
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

	err := ns.latestErr.Load().(error)
	if errors.Is(err, errNil) {
		err = nil
	}
	return msgs, err
}

func (ns *jsSource) Pending(ctx context.Context) (int64, error) {
	info, err := ns.consumer.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting consumer info: %w", err)
	}
	return int64(info.NumPending), nil
}

func (ns *jsSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	errs := make([]error, len(offsets))
	for i, o := range offsets {
		if err := o.AckIt(); err != nil {
			errs[i] = err
		}
	}
	return errs
}

func (ns *jsSource) Close() error {
	ns.logger.Info("Shutting down Jetstream source server...")
	// Using Stop instead of Drain to discard all messages in the buffer, since SourceReader.Read won't be called after invoking SourceReader.Close
	ns.consumerHandle.Stop()
	ns.natsConn.Close()
	ns.logger.Info("Jetstream source server shutdown")
	return nil
}
