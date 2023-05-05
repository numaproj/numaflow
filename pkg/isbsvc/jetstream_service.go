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

package isbsvc

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/jetstream"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type jetStreamSvc struct {
	pipelineName string

	jsClient jsclient.JetStreamClient
	js       *jsclient.JetStreamContext
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

func WithJetStreamClient(jsClient jsclient.JetStreamClient) JSServiceOption {
	return func(j *jetStreamSvc) error {
		j.jsClient = jsClient
		return nil
	}
}

func (jss *jetStreamSvc) CreateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, opts ...CreateOption) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}
	log := logging.FromContext(ctx)
	creatOpts := &createOptions{}
	for _, opt := range opts {
		if err := opt(creatOpts); err != nil {
			return err
		}
	}
	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewBufferString(creatOpts.config)); err != nil {
		return err
	}

	nc, err := jsclient.NewInClusterJetStreamClient().Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get a js context from nats connection, %w", err)
	}
	for _, buffer := range buffers {
		streamName := JetStreamName(buffer)
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
				Storage:    nats.StorageType(v.GetInt("stream.storage")),
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
		for _, bucket := range buckets {
			// Create offset-timeline bucket
			otBucket := JetStreamOTBucket(bucket)
			if _, err := js.KeyValue(otBucket); err != nil {
				if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
					return fmt.Errorf("failed to query information of bucket %q during buffer creating, %w", otBucket, err)
				}
				if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
					Bucket:       otBucket,
					MaxValueSize: v.GetInt32("otBucket.maxValueSize"),
					History:      uint8(v.GetUint("otBucket.history")),
					TTL:          v.GetDuration("otBucket.ttl"),
					MaxBytes:     v.GetInt64("otBucket.maxBytes"),
					Storage:      nats.StorageType(v.GetInt("otBucket.storage")),
					Replicas:     v.GetInt("otBucket.replicas"),
					Placement:    nil,
				}); err != nil {
					return fmt.Errorf("failed to create offset timeline bucket %q, %w", otBucket, err)
				}
			}
			// Create processor bucket
			procBucket := JetStreamProcessorBucket(bucket)
			if _, err := js.KeyValue(procBucket); err != nil {
				if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
					return fmt.Errorf("failed to query information of bucket %q during buffer creating, %w", procBucket, err)
				}
				if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
					Bucket:       procBucket,
					MaxValueSize: v.GetInt32("procBucket.maxValueSize"),
					History:      uint8(v.GetUint("procBucket.history")),
					TTL:          v.GetDuration("procBucket.ttl"),
					MaxBytes:     v.GetInt64("procBucket.maxBytes"),
					Storage:      nats.StorageType(v.GetInt("procBucket.storage")),
					Replicas:     v.GetInt("procBucket.replicas"),
					Placement:    nil,
				}); err != nil {
					return fmt.Errorf("failed to create processor bucket %q, %w", otBucket, err)
				}
			}
		}
	}
	return nil
}

func (jss *jetStreamSvc) DeleteBuffersAndBuckets(ctx context.Context, buffers, buckets []string) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}
	log := logging.FromContext(ctx)
	nc, err := jsclient.NewInClusterJetStreamClient().Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get a js context from nats connection, %w", err)
	}
	for _, buffer := range buffers {
		streamName := JetStreamName(buffer)
		if err := js.DeleteStream(streamName); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete stream %q, %w", streamName, err)
		}
		log.Infow("Succeeded to delete a stream", zap.String("stream", streamName))
	}
	for _, bucket := range buckets {
		otBucket := JetStreamOTBucket(bucket)
		if err := js.DeleteKeyValue(otBucket); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete offset timeline bucket %q, %w", otBucket, err)
		}
		log.Infow("Succeeded to delete an offset timeline bucket", zap.String("bucket", otBucket))
		procBucket := JetStreamProcessorBucket(bucket)
		if err := js.DeleteKeyValue(procBucket); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete processor bucket %q, %w", procBucket, err)
		}
		log.Infow("Succeeded to delete a processor bucket", zap.String("bucket", procBucket))
	}
	return nil
}

func (jss *jetStreamSvc) ValidateBuffersAndBuckets(ctx context.Context, buffers, buckets []string) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}
	nc, err := jsclient.NewInClusterJetStreamClient().Connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return fmt.Errorf("failed to get a js context from nats connection, %w", err)
	}
	for _, buffer := range buffers {
		streamName := JetStreamName(buffer)
		if _, err := js.StreamInfo(streamName); err != nil {
			return fmt.Errorf("failed to query information of stream %q, %w", streamName, err)
		}
	}
	for _, bucket := range buckets {
		otBucket := JetStreamOTBucket(bucket)
		if _, err := js.KeyValue(otBucket); err != nil {
			return fmt.Errorf("failed to query OT bucket %q, %w", otBucket, err)
		}

		procBucket := JetStreamProcessorBucket(bucket)
		if _, err := js.KeyValue(procBucket); err != nil {
			return fmt.Errorf("failed to query processor bucket %q, %w", procBucket, err)
		}
	}
	return nil
}

func (jss *jetStreamSvc) GetBufferInfo(ctx context.Context, buffer string) (*BufferInfo, error) {
	var js *jsclient.JetStreamContext
	if jss.js != nil { // Daemon server use case
		js = jss.js
	} else if jss.jsClient != nil { // Daemon server first time access use case
		nc, err := jss.jsClient.Connect(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get nats connection with given JetStream client, %w", err)
		}
		js, err = nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to get a JetStream context from nats connection, %w", err)
		}
		jss.js = js
	} else { // Short running use case
		nc, err := jsclient.NewInClusterJetStreamClient().Connect(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
		}
		defer nc.Close()
		js, err = nc.JetStream()
		if err != nil {
			return nil, fmt.Errorf("failed to get a JetStream context from nats connection, %w", err)
		}
	}
	streamName := JetStreamName(buffer)
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

func (jss *jetStreamSvc) CreateWatermarkFetcher(ctx context.Context, bucketName string) (fetch.Fetcher, error) {
	hbBucketName := JetStreamProcessorBucket(bucketName)
	hbWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, jss.pipelineName, hbBucketName, jss.jsClient)
	if err != nil {
		return nil, err
	}
	otBucketName := JetStreamOTBucket(bucketName)
	otWatch, err := jetstream.NewKVJetStreamKVWatch(ctx, jss.pipelineName, otBucketName, jss.jsClient)
	if err != nil {
		return nil, err
	}
	watermarkFetcher := fetch.NewEdgeFetcher(ctx, bucketName, store.BuildWatermarkStoreWatcher(hbWatch, otWatch))
	return watermarkFetcher, nil
}

func JetStreamName(bufferName string) string {
	return bufferName
}

func JetStreamOTBucket(bucketName string) string {
	return fmt.Sprintf("%s_OT", bucketName)
}

func JetStreamProcessorBucket(bucketName string) string {
	return fmt.Sprintf("%s_PROCESSORS", bucketName)
}
