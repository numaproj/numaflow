package isbsvc

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type jetStreamSvc struct {
	pipelineName string
	js           nats.JetStreamContext
}

func NewISBJetStreamSvc(pipelineName string, opts ...JSServiceOption) (ISBService, error) {
	j := &jetStreamSvc{pipelineName: pipelineName}
	for _, o := range opts {
		if err := o(j); err != nil {
			return nil, err
		}
	}
	return j, nil
}

type JSServiceOption func(*jetStreamSvc) error

func WithNatsConnection(conn *nats.Conn) JSServiceOption {
	return func(j *jetStreamSvc) error {
		js, err := conn.JetStream()
		if err != nil {
			return err
		}
		j.js = js
		return nil
	}
}

func (jss *jetStreamSvc) CreateBuffers(ctx context.Context, buffers []string, opts ...BufferCreateOption) error {
	log := logging.FromContext(ctx)
	bufferCreatOpts := &bufferCreateOptions{}
	for _, opt := range opts {
		if err := opt(bufferCreatOpts); err != nil {
			return err
		}
	}
	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewBufferString(bufferCreatOpts.bufferConfig)); err != nil {
		return err
	}

	nc, err := clients.NewInClusterJetStreamClient().Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get a js context from nats connection, %w", err)
	}
	for _, b := range buffers {
		// Create a stream for each buffer
		streamName := streamName(jss.pipelineName, b)
		_, err := js.StreamInfo(streamName)
		if err != nil {
			if !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of stream %q during buffer creating, %w", streamName, err)
			}
			if _, err = js.AddStream(&nats.StreamConfig{
				Name:       streamName,
				Subjects:   []string{streamName}, // Use the stream name as the only subject
				Retention:  nats.RetentionPolicy(v.GetInt("stream.retention")),
				Discard:    nats.DiscardOld,
				MaxMsgs:    v.GetInt64("stream.maxMsgs"),
				MaxAge:     v.GetDuration("stream.maxAge"),
				MaxBytes:   v.GetInt64("stream.maxBytes"),
				Storage:    nats.FileStorage,
				Replicas:   v.GetInt("stream.replicas"),
				Duplicates: v.GetDuration("stream.duplicates"), // No duplication in this period
			}); err != nil {
				return fmt.Errorf("failed to create stream %q and buffers, %w", streamName, err)
			}
			log.Infow("Succeeded to create a stream and buffers", zap.String("stream", streamName), zap.Strings("buffers", []string{streamName}))
			if _, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
				Durable:       streamName,
				DeliverPolicy: nats.DeliverAllPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
				AckWait:       v.GetDuration("consumer.ackWait"),
				MaxAckPending: v.GetInt("consumer.maxAckPending"),
				FilterSubject: streamName,
			}); err != nil {
				return fmt.Errorf("failed to create a consumer for stream %q, %w", streamName, err)
			}
			log.Infow("Succeeded to create a consumer for a stream", zap.String("stream", streamName), zap.String("consumer", streamName))
		}
	}
	return nil
}

func (jss *jetStreamSvc) DeleteBuffers(ctx context.Context, buffers []string) error {
	log := logging.FromContext(ctx)
	nc, err := clients.NewInClusterJetStreamClient().Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get a js context from nats connection, %w", err)
	}
	for _, b := range buffers {
		streamName := fmt.Sprintf("%s-%s", jss.pipelineName, b)
		if err := js.DeleteStream(streamName); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete stream %q, %w", streamName, err)
		}
		log.Infow("succeeded to delete a stream", zap.String("stream", streamName))
	}
	return nil
}

func (jss *jetStreamSvc) ValidateBuffers(ctx context.Context, buffers []string) error {
	nc, err := clients.NewInClusterJetStreamClient().Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get a js context from nats connection, %w", err)
	}
	for _, b := range buffers {
		streamName := fmt.Sprintf("%s-%s", jss.pipelineName, b)
		_, err := js.StreamInfo(streamName)
		if err != nil {
			return fmt.Errorf("failed to query information of stream %q, %w", streamName, err)
		}
	}
	return nil
}

func (jss *jetStreamSvc) GetBufferInfo(ctx context.Context, buffer string) (*BufferInfo, error) {
	var js nats.JetStreamContext
	if jss.js != nil {
		js = jss.js
	} else {
		nc, err := clients.NewInClusterJetStreamClient().Connect(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
		}
		defer nc.Close()
		js, err = nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to get a JetStream context from nats connection, %w", err)
		}
	}
	streamName := streamName(jss.pipelineName, buffer)
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get information of stream %q", streamName)
	}
	consumer, err := js.ConsumerInfo(streamName, streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer information of stream %q", streamName)
	}
	totalMessages := int64(stream.State.Msgs)
	if stream.Config.Retention == nats.LimitsPolicy {
		totalMessages = int64(consumer.NumPending) + int64(consumer.NumAckPending)
	}
	bufferInfo := &BufferInfo{
		Name:            buffer,
		PendingCount:    int64(consumer.NumPending),
		AckPendingCount: int64(consumer.NumAckPending),
		TotalMessages:   totalMessages,
	}
	return bufferInfo, nil
}

func streamName(pipelineName, bufferName string) string {
	return fmt.Sprintf("%s-%s", pipelineName, bufferName)
}
