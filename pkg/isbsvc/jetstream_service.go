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

func NewISBJetStreamSvc(pipelineName string, jsClient *jsclient.Client) (ISBService, error) {
	jsCtx, err := jsClient.JetStreamContext()
	if err != nil {
		return nil, fmt.Errorf("failed to get a JetStream context from nats connection, %w", err)
	}

	j := &jetStreamSvc{
		pipelineName: pipelineName,
		jsClient:     jsClient,
		js:           jsCtx,
	}
	return j, nil
}

func (jss *jetStreamSvc) CreateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStreams []string, opts ...CreateOption) error {
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

	if sideInputsStore != "" {
		kvName := JetStreamSideInputsStoreKVName(sideInputsStore)
		if _, err := jss.js.KeyValue(kvName); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of KV %q, %w", kvName, err)
			}
			if _, err := jss.js.CreateKeyValue(&nats.KeyValueConfig{
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

	if len(servingSourceStreams) > 0 {
		for _, servingSourceStream := range servingSourceStreams {
			_, err := jss.js.StreamInfo(servingSourceStream)
			if err != nil {
				if !errors.Is(err, nats.ErrStreamNotFound) {
					return fmt.Errorf("failed to query information of stream %q during buffer creating, %w", servingSourceStream, err)
				}
				if _, err := jss.js.AddStream(&nats.StreamConfig{
					Name:       servingSourceStream,
					Subjects:   []string{servingSourceStream}, // Use the stream name as the only subject
					Storage:    nats.StorageType(v.GetInt("stream.storage")),
					Replicas:   v.GetInt("stream.replicas"),
					Retention:  nats.WorkQueuePolicy, // we can delete the message immediately after it's consumed and acked
					MaxMsgs:    -1,                   // unlimited messages
					MaxBytes:   -1,                   // unlimited bytes
					Duplicates: v.GetDuration("stream.duplicates"),
				}); err != nil {
					return fmt.Errorf("failed to create serving source stream %q, %w", servingSourceStream, err)
				}
			}
		}
	}

	for _, buffer := range buffers {
		streamName := JetStreamName(buffer)
		_, err := jss.js.StreamInfo(streamName)
		if err != nil {
			if !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of stream %q during buffer creating, %w", streamName, err)
			}
			if _, err := jss.js.AddStream(&nats.StreamConfig{
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
			if _, err := jss.js.AddConsumer(streamName, &nats.ConsumerConfig{
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
		if _, err := jss.js.KeyValue(otKVName); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of bucket %q during buffer creating, %w", otKVName, err)
			}
			if _, err := jss.js.CreateKeyValue(&nats.KeyValueConfig{
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
		if _, err := jss.js.KeyValue(procKVName); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of bucket %q during buffer creating, %w", procKVName, err)
			}
			if _, err := jss.js.CreateKeyValue(&nats.KeyValueConfig{
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

func (jss *jetStreamSvc) DeleteBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStreams []string) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}
	log := logging.FromContext(ctx)
	for _, buffer := range buffers {
		streamName := JetStreamName(buffer)
		if err := jss.js.DeleteStream(streamName); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete stream %q, %w", streamName, err)
		}
		log.Infow("Succeeded to delete a stream", zap.String("stream", streamName))
	}
	for _, bucket := range buckets {
		otKVName := wmstore.JetStreamOTKVName(bucket)
		if err := jss.js.DeleteKeyValue(otKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete offset timeline KV %q, %w", otKVName, err)
		}
		log.Infow("Succeeded to delete an offset timeline KV", zap.String("kvName", otKVName))
		procKVName := wmstore.JetStreamProcessorKVName(bucket)
		if err := jss.js.DeleteKeyValue(procKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete processor KV %q, %w", procKVName, err)
		}
		log.Infow("Succeeded to delete a processor KV", zap.String("kvName", procKVName))
	}

	if sideInputsStore != "" {
		sideInputsKVName := JetStreamSideInputsStoreKVName(sideInputsStore)
		if err := jss.js.DeleteKeyValue(sideInputsKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete side inputs KV %q, %w", sideInputsKVName, err)
		}
		log.Infow("Succeeded to delete a side inputs KV", zap.String("kvName", sideInputsKVName))
	}

	if len(servingSourceStreams) > 0 {
		for _, servingSourceStream := range servingSourceStreams {
			if err := jss.js.DeleteStream(servingSourceStream); err != nil && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to delete serving source stream %q, %w", servingSourceStream, err)
			}
			log.Infow("Succeeded to delete the serving source stream", zap.String("stream", servingSourceStream))
		}
	}
	return nil
}

func (jss *jetStreamSvc) ValidateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStreams []string) error {
	if len(buffers) == 0 && len(buckets) == 0 {
		return nil
	}

	for _, buffer := range buffers {
		streamName := JetStreamName(buffer)
		if _, err := jss.js.StreamInfo(streamName); err != nil {
			return fmt.Errorf("failed to query information of stream %q, %w", streamName, err)
		}
	}
	for _, bucket := range buckets {
		otKVName := wmstore.JetStreamOTKVName(bucket)
		if _, err := jss.js.KeyValue(otKVName); err != nil {
			return fmt.Errorf("failed to query OT KV %q, %w", otKVName, err)
		}

		procKVName := wmstore.JetStreamProcessorKVName(bucket)
		if _, err := jss.js.KeyValue(procKVName); err != nil {
			return fmt.Errorf("failed to query processor KV %q, %w", procKVName, err)
		}
	}
	if sideInputsStore != "" {
		sideInputsKVName := JetStreamSideInputsStoreKVName(sideInputsStore)
		if _, err := jss.js.KeyValue(sideInputsKVName); err != nil {
			return fmt.Errorf("failed to query side inputs store KV %q, %w", sideInputsKVName, err)
		}
	}
	if len(servingSourceStreams) > 0 {
		for _, servingSourceStream := range servingSourceStreams {
			if _, err := jss.js.StreamInfo(servingSourceStream); err != nil {
				return fmt.Errorf("failed to query information of stream %q, %w", servingSourceStream, err)
			}
		}
	}
	return nil
}

func (jss *jetStreamSvc) GetBufferInfo(ctx context.Context, buffer string) (*BufferInfo, error) {
	streamName := JetStreamName(buffer)
	stream, err := jss.js.StreamInfo(streamName)
	if err != nil {
		return nil, fmt.Errorf("failed to get information of stream %q", streamName)
	}
	consumer, err := jss.js.ConsumerInfo(streamName, streamName)
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
