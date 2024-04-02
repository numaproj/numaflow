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
	"time"

	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	wmstore "github.com/numaproj/numaflow/pkg/watermark/store"
)

type jetStreamSvc struct {
	pipelineName string
	jsClient     *jsclient.Client
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

func WithJetStreamClient(jsClient *jsclient.Client) JSServiceOption {
	return func(j *jetStreamSvc) error {
		j.jsClient = jsClient
		return nil
	}
}

func (jss *jetStreamSvc) CreateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, opts ...CreateOption) error {
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

	nc, err := jsclient.NewNATSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStreamContext()
	if err != nil {
		return fmt.Errorf("failed to get a js context from nats connection, %w", err)
	}
	if sideInputsStore != "" {
		kvName := JetStreamSideInputsStoreKVName(sideInputsStore)
		if _, err := js.KeyValue(kvName); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of KV %q, %w", kvName, err)
			}
			if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:       kvName,
				MaxValueSize: 0,
				History:      1,                   // No history
				TTL:          time.Hour * 24 * 30, // 30 days
				MaxBytes:     0,
				Storage:      nats.FileStorage,
				Replicas:     v.GetInt("stream.replicas"),
			}); err != nil {
				return fmt.Errorf("failed to create side inputs KV %q, %w", kvName, err)
			}
			log.Infow("Succeeded to create a side inputs KV", zap.String("kvName", kvName))
		}
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
			log.Infow("Succeeded to create a stream", zap.String("stream", streamName))
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

	for _, bucket := range buckets {
		// Create offset-timeline KV
		otKVName := wmstore.JetStreamOTKVName(bucket)
		if _, err := js.KeyValue(otKVName); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of bucket %q during buffer creating, %w", otKVName, err)
			}
			if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:       otKVName,
				MaxValueSize: v.GetInt32("otBucket.maxValueSize"),
				History:      uint8(v.GetUint("otBucket.history")),
				TTL:          v.GetDuration("otBucket.ttl"),
				MaxBytes:     v.GetInt64("otBucket.maxBytes"),
				Storage:      nats.StorageType(v.GetInt("otBucket.storage")),
				Replicas:     v.GetInt("otBucket.replicas"),
				Placement:    nil,
			}); err != nil {
				return fmt.Errorf("failed to create offset timeline KV %q, %w", otKVName, err)
			}
		}
		// Create processor KV
		procKVName := wmstore.JetStreamProcessorKVName(bucket)
		if _, err := js.KeyValue(procKVName); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of bucket %q during buffer creating, %w", procKVName, err)
			}
			if _, err := js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:       procKVName,
				MaxValueSize: v.GetInt32("procBucket.maxValueSize"),
				History:      uint8(v.GetUint("procBucket.history")),
				TTL:          v.GetDuration("procBucket.ttl"),
				MaxBytes:     v.GetInt64("procBucket.maxBytes"),
				Storage:      nats.StorageType(v.GetInt("procBucket.storage")),
				Replicas:     v.GetInt("procBucket.replicas"),
				Placement:    nil,
			}); err != nil {
				return fmt.Errorf("failed to create processor KV %q, %w", procKVName, err)
			}
		}
	}
	return nil
}

func (jss *jetStreamSvc) DeleteBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}
	log := logging.FromContext(ctx)
	nc, err := jsclient.NewNATSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStreamContext()
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
		otKVName := wmstore.JetStreamOTKVName(bucket)
		if err := js.DeleteKeyValue(otKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete offset timeline KV %q, %w", otKVName, err)
		}
		log.Infow("Succeeded to delete an offset timeline KV", zap.String("kvName", otKVName))
		procKVName := wmstore.JetStreamProcessorKVName(bucket)
		if err := js.DeleteKeyValue(procKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete processor KV %q, %w", procKVName, err)
		}
		log.Infow("Succeeded to delete a processor KV", zap.String("kvName", procKVName))
	}

	if sideInputsStore != "" {
		sideInputsKVName := JetStreamSideInputsStoreKVName(sideInputsStore)
		if err := js.DeleteKeyValue(sideInputsKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete side inputs KV %q, %w", sideInputsKVName, err)
		}
		log.Infow("Succeeded to delete a side inputs KV", zap.String("kvName", sideInputsKVName))
	}
	return nil
}

func (jss *jetStreamSvc) ValidateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}
	nc, err := jsclient.NewNATSClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
	}
	defer nc.Close()
	js, err := nc.JetStreamContext()
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
		otKVName := wmstore.JetStreamOTKVName(bucket)
		if _, err := js.KeyValue(otKVName); err != nil {
			return fmt.Errorf("failed to query OT KV %q, %w", otKVName, err)
		}

		procKVName := wmstore.JetStreamProcessorKVName(bucket)
		if _, err := js.KeyValue(procKVName); err != nil {
			return fmt.Errorf("failed to query processor KV %q, %w", procKVName, err)
		}
	}
	if sideInputsStore != "" {
		sideInputsKVName := JetStreamSideInputsStoreKVName(sideInputsStore)
		if _, err := js.KeyValue(sideInputsKVName); err != nil {
			return fmt.Errorf("failed to query side inputs store KV %q, %w", sideInputsKVName, err)
		}
	}
	return nil
}

func (jss *jetStreamSvc) GetBufferInfo(ctx context.Context, buffer string) (*BufferInfo, error) {
	var js nats.JetStreamContext
	var err error
	if jss.js != nil { // Daemon server use case
		js = jss.js
	} else if jss.jsClient != nil { // Daemon server first time access use case
		js, err = jss.jsClient.JetStreamContext()
		if err != nil {
			return nil, fmt.Errorf("failed to get a JetStream context from nats connection, %w", err)
		}
		jss.js = js
	} else { // Short running use case
		nc, err := jsclient.NewNATSClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
		}
		defer nc.Close()
		js, err = nc.JetStreamContext()
		if err != nil {
			return nil, fmt.Errorf("failed to get a JetStream context from nats connection, %w", err)
		}
		jss.js = js
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

// CreateWatermarkStores is used to create watermark stores.
func (jss *jetStreamSvc) CreateWatermarkStores(ctx context.Context, bucketName string, fromBufferPartitionCount int, isReduce bool) ([]wmstore.WatermarkStore, error) {
	log := logging.FromContext(ctx).With("bucket", bucketName)
	ctx = logging.WithLogger(ctx, log)
	var wmStores []wmstore.WatermarkStore
	partitions := 1
	if isReduce {
		partitions = fromBufferPartitionCount
	}
	// if it's not a reduce vertex, we only need one store to store the watermark
	for i := 0; i < partitions; i++ {
		wmStore, err := wmstore.BuildJetStreamWatermarkStore(ctx, bucketName, jss.jsClient)
		if err != nil {
			return nil, fmt.Errorf("failed to create new JetStream watermark store, %w", err)
		}
		wmStores = append(wmStores, wmStore)
	}
	return wmStores, nil
}

func JetStreamName(bufferName string) string {
	return bufferName
}

func JetStreamSideInputsStoreKVName(sideInputStoreName string) string {
	return fmt.Sprintf("%s_SIDE_INPUTS", sideInputStoreName)
}
