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
		streamName := JetStreamName(jss.pipelineName, b)
		_, err := js.StreamInfo(streamName)
		if err != nil {
			if !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of stream %q during buffer creating, %w", streamName, err)
			}
			if _, err := js.AddStream(&nats.StreamConfig{
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
		// Create offset-timeline bucket
		otBucket := JetStreamOTBucket(jss.pipelineName, b)
		if _, err := js.KeyValue(otBucket); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) {
				return fmt.Errorf("failed to query information of bucket %q during buffer creating, %w", otBucket, err)
			}
			if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:       otBucket,
				Description:  fmt.Sprintf("Offset timeline bucket of buffer [%s]", b),
				MaxValueSize: v.GetInt32("otBucket.maxValueSize"),
				History:      uint8(v.GetUint("otBucket.history")),
				TTL:          v.GetDuration("otBucket.ttl"),
				MaxBytes:     v.GetInt64("otBucket.maxBytes"),
				Storage:      nats.FileStorage,
				Replicas:     v.GetInt("otBucket.replicas"),
				Placement:    nil,
			}); err != nil {
				return fmt.Errorf("failed to create offset timeline bucket %q, %w", otBucket, err)
			}
		}
		// Create processor bucket
		procBucket := JetStreamProcessorBucket(jss.pipelineName, b)
		if _, err := js.KeyValue(procBucket); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) {
				return fmt.Errorf("failed to query information of bucket %q during buffer creating, %w", procBucket, err)
			}
			if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:       procBucket,
				Description:  fmt.Sprintf("Processor bucket of buffer [%s]", b),
				MaxValueSize: v.GetInt32("procBucket.maxValueSize"),
				History:      uint8(v.GetUint("procBucket.history")),
				TTL:          v.GetDuration("procBucket.ttl"),
				MaxBytes:     v.GetInt64("procBucket.maxBytes"),
				Storage:      nats.FileStorage,
				Replicas:     v.GetInt("procBucket.replicas"),
				Placement:    nil,
			}); err != nil {
				return fmt.Errorf("failed to create processor bucket %q, %w", otBucket, err)
			}
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
		streamName := JetStreamName(jss.pipelineName, b)
		if err := js.DeleteStream(streamName); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete stream %q, %w", streamName, err)
		}
		log.Infow("Succeeded to delete a stream", zap.String("stream", streamName))
		otBucket := JetStreamOTBucket(jss.pipelineName, b)
		if err := js.DeleteKeyValue(otBucket); err != nil && !errors.Is(err, nats.ErrBucketNotFound) {
			return fmt.Errorf("failed to delete offset timeline bucket %q, %w", otBucket, err)
		}
		log.Infow("Succeeded to delete an offset timeline bucket", zap.String("bucket", otBucket))
		procBucket := JetStreamProcessorBucket(jss.pipelineName, b)
		if err := js.DeleteKeyValue(procBucket); err != nil && !errors.Is(err, nats.ErrBucketNotFound) {
			return fmt.Errorf("failed to delete processor bucket %q, %w", procBucket, err)
		}
		log.Infow("Succeeded to delete a processor bucket", zap.String("bucket", procBucket))
	}
	return nil
}

func (jss *jetStreamSvc) ValidateBuffers(ctx context.Context, buffers []string) error {
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
		streamName := JetStreamName(jss.pipelineName, b)
		if _, err := js.StreamInfo(streamName); err != nil {
			return fmt.Errorf("failed to query information of stream %q, %w", streamName, err)
		}

		otBucket := JetStreamOTBucket(jss.pipelineName, b)
		if _, err := js.KeyValue(otBucket); err != nil {
			// TODO: throw an error
			log.Warnw("Failed to query bucket", zap.String("bucket", otBucket), zap.Error(err))
		}

		procBucket := JetStreamProcessorBucket(jss.pipelineName, b)
		if _, err := js.KeyValue(procBucket); err != nil {
			// TODO: throw an error
			log.Warnw("Failed to query bucket", zap.String("bucket", procBucket), zap.Error(err))
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
	streamName := JetStreamName(jss.pipelineName, buffer)
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

func JetStreamName(pipelineName, bufferName string) string {
	return fmt.Sprintf("%s-%s", pipelineName, bufferName)
}

func JetStreamOTBucket(pipelineName, bufferName string) string {
	return fmt.Sprintf("%s-%s_OT", pipelineName, bufferName)
}

func JetStreamProcessorBucket(pipelineName, bufferName string) string {
	return fmt.Sprintf("%s-%s_PROCESSORS", pipelineName, bufferName)
}
