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
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/sources/sourcer"
)

// FIXME: handle context.
type natsSource struct {
	vertexName    string
	pipelineName  string
	vertexReplica int32
	logger        *zap.SugaredLogger
	natsConn      *natslib.Conn
	sub           *natslib.Subscription
	bufferSize    int
	messages      chan *isb.ReadMessage
	readTimeout   time.Duration
}

func New(ctx context.Context, vertexInstance *dfv1.VertexInstance, opts ...Option) (sourcer.SourceReader, error) {

	n := &natsSource{
		vertexName:    vertexInstance.Vertex.Spec.Name,
		pipelineName:  vertexInstance.Vertex.Spec.PipelineName,
		vertexReplica: vertexInstance.Replica,
		bufferSize:    1000,            // default size
		readTimeout:   1 * time.Second, // default timeout
		logger:        logging.FromContext(ctx),
	}
	for _, o := range opts {
		if err := o(n); err != nil {
			return nil, err
		}
	}

	n.messages = make(chan *isb.ReadMessage, n.bufferSize)

	source := vertexInstance.Vertex.Spec.Source.Nats
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
	if conn, err := natslib.Connect(source.URL, opt...); err != nil {
		return nil, fmt.Errorf("failed to connect to nats server, %w", err)
	} else {
		n.natsConn = conn
	}
	if sub, err := n.natsConn.QueueSubscribe(source.Subject, source.Queue, func(msg *natslib.Msg) {
		readOffset := isb.NewSimpleStringPartitionOffset(uuid.New().String(), vertexInstance.Replica)
		m := &isb.ReadMessage{
			Message: isb.Message{
				Header: isb.Header{
					// TODO: Be able to specify event time.
					MessageInfo: isb.MessageInfo{EventTime: time.Now()},
					ID: isb.MessageID{
						VertexName: n.vertexName,
						Offset:     readOffset.String(),
						Index:      readOffset.PartitionIdx(),
					},
				},
				Body: isb.Body{
					Payload: msg.Data,
				},
			},
			// TODO: Be able to specify an ID for dedup?
			ReadOffset: readOffset,
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
	return ns.vertexName
}

// Partitions returns the partitions associated with this source.
func (ns *natsSource) Partitions(context.Context) []int32 {
	return []int32{ns.vertexReplica}
}

func (ns *natsSource) Read(_ context.Context, count int64) ([]*isb.ReadMessage, error) {
	var msgs []*isb.ReadMessage
	timeout := time.After(ns.readTimeout)
loop:
	for i := int64(0); i < count; i++ {
		select {
		case m := <-ns.messages:
			natsSourceReadCount.With(map[string]string{metrics.LabelVertex: ns.vertexName, metrics.LabelPipeline: ns.pipelineName}).Inc()
			msgs = append(msgs, m)
		case <-timeout:
			ns.logger.Debugw("Timed out waiting for messages to read.", zap.Duration("waited", ns.readTimeout), zap.Int("read", len(msgs)))
			break loop
		}
	}
	ns.logger.Debugf("Read %d messages.", len(msgs))
	return msgs, nil
}

func (ns *natsSource) Pending(_ context.Context) (int64, error) {
	return isb.PendingNotAvailable, nil
}

func (ns *natsSource) Ack(_ context.Context, offsets []isb.Offset) []error {
	return make([]error, len(offsets))
}

func (ns *natsSource) Close() error {
	ns.logger.Info("Shutting down nats source server...")
	if err := ns.sub.Unsubscribe(); err != nil {
		ns.logger.Errorw("Failed to unsubscribe nats subscription", zap.Error(err))
	}
	ns.natsConn.Close()
	ns.logger.Info("Nats source server shutdown")
	return nil
}
