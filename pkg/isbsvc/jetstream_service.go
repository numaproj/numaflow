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
)

type jetStreamSvc struct {
	jsClient *jsclient.Client
	js       nats.JetStreamContext
}

func NewISBJetStreamSvc(jsClient *jsclient.Client) (ISBService, error) {
	jsCtx, err := jsClient.JetStreamContext()
	if err != nil {
		return nil, fmt.Errorf("failed to get a JetStream context from nats connection, %w", err)
	}

	j := &jetStreamSvc{
		jsClient: jsClient,
		js:       jsCtx,
	}
	return j, nil
}

func (jss *jetStreamSvc) CreateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStore string, opts ...CreateOption) error {
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

	if servingSourceStore != "" {
		cbStoreName := JetStreamServingCallbackStoreName(servingSourceStore)
		if _, err := jss.js.KeyValue(cbStoreName); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of KV %q, %w", cbStoreName, err)
			}
			if _, err := jss.js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:       cbStoreName,
				MaxValueSize: 0,
				History:      1,
				TTL:          30 * time.Minute,
				MaxBytes:     0,
				Storage:      nats.FileStorage,
				Replicas:     v.GetInt("stream.replicas"),
			}); err != nil {
				return fmt.Errorf("failed to create serving source KV %q, %w", cbStoreName, err)
			}
			log.Infow("Succeeded to create a serving source Callback KV", zap.String("kvName", cbStoreName))
		}
		rsStoreName := JetStreamServingResponseStoreName(servingSourceStore)
		if _, err := jss.js.KeyValue(rsStoreName); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of KV %q, %w", rsStoreName, err)
			}
			if _, err := jss.js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:       rsStoreName,
				MaxValueSize: 0,
				History:      1,
				TTL:          2 * time.Hour,
				MaxBytes:     0,
				Storage:      nats.FileStorage,
				Replicas:     v.GetInt("stream.replicas"),
			}); err != nil {
				return fmt.Errorf("failed to create serving source KV %q, %w", rsStoreName, err)
			}
			log.Infow("Succeeded to create a serving source Response KV", zap.String("kvName", rsStoreName))
		}
		statusStoreName := JetStreamServingStatusStoreName(servingSourceStore)
		if _, err := jss.js.KeyValue(statusStoreName); err != nil {
			if !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of KV %q, %w", statusStoreName, err)
			}
			if _, err := jss.js.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:       statusStoreName,
				MaxValueSize: 0,
				History:      1,
				TTL:          2 * time.Hour,
				MaxBytes:     0,
				Storage:      nats.FileStorage,
				Replicas:     v.GetInt("stream.replicas"),
			}); err != nil {
				return fmt.Errorf("failed to create serving source KV %q, %w", statusStoreName, err)
			}
			log.Infow("Succeeded to create a serving source Status KV", zap.String("kvName", statusStoreName))
		}
	}

	for _, buffer := range buffers {
		streamName := JetStreamName(buffer)
		_, err := jss.js.StreamInfo(streamName)
		if err != nil {
			if !errors.Is(err, nats.ErrStreamNotFound) {
				return fmt.Errorf("failed to query information of stream %q during buffer creating, %w", streamName, err)
			}
			// get the retention policy from the stream config
			retention := nats.RetentionPolicy(v.GetInt("stream.retention"))
			discard := nats.DiscardNew

			// Based on the retention policy we use the following discard policy
			// 1) Limits Policy -> DiscardOld
			// 2) WorkQueuePolicy/Interest -> DiscardNew

			// In WorkQueuePolicy the messages will be removed as soon as the Consumer received an Acknowledgement.
			// In InterestPolicy messages will be removed as soon as all Consumers of the stream for that subject have
			// received an Acknowledgement for the message.
			// For Numaflow, workqueue and interest is the same, because we only have one consumer
			// Old messages should be deleted once, they are acknowledged, hence we use DiscardNew with these two
			// policies in which during a buffer full we will not write more message to the stream and wait
			// for the older messages to get cleared

			// When operating with DiscardNew and Limits, on reaching the maxMsgs limit, it will result in the stream
			// returning an error when attempting to write new messages and old messages will not be deleted from the stream
			// so the pipeline will get stuck. Hence, we cannot use Limits with DiscardNew.
			//
			if retention == nats.LimitsPolicy {
				discard = nats.DiscardOld
			}

			if _, err := jss.js.AddStream(&nats.StreamConfig{
				Name:       streamName,
				Subjects:   []string{streamName}, // Use the stream name as the only subject
				Retention:  retention,
				Discard:    discard,
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
		}

		_, err = jss.js.ConsumerInfo(streamName, streamName)
		if err != nil {
			if !errors.Is(err, nats.ErrConsumerNotFound) {
				return fmt.Errorf("failed to query information of consumer for stream %q, %w", streamName, err)
			}

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
		otKVName := JetStreamOTKVName(bucket)
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
		procKVName := JetStreamProcessorKVName(bucket)
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

func (jss *jetStreamSvc) DeleteBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStore string) error {
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
		otKVName := JetStreamOTKVName(bucket)
		if err := jss.js.DeleteKeyValue(otKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete offset timeline KV %q, %w", otKVName, err)
		}
		log.Infow("Succeeded to delete an offset timeline KV", zap.String("kvName", otKVName))
		procKVName := JetStreamProcessorKVName(bucket)
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

	if servingSourceStore != "" {
		servingSourceCallbackStoreKVName := JetStreamServingCallbackStoreName(servingSourceStore)
		if err := jss.js.DeleteKeyValue(servingSourceCallbackStoreKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete serving source callback store %q, %w", servingSourceCallbackStoreKVName, err)
		}
		log.Infow("Succeeded to delete a serving source store", zap.String("kvName", servingSourceCallbackStoreKVName))
		servingSourceResponseStoreKVName := JetStreamServingResponseStoreName(servingSourceStore)
		if err := jss.js.DeleteKeyValue(servingSourceResponseStoreKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete serving source response store %q, %w", servingSourceResponseStoreKVName, err)
		}
		servingSourceStatusStoreKVName := JetStreamServingStatusStoreName(servingSourceStore)
		if err := jss.js.DeleteKeyValue(servingSourceStatusStoreKVName); err != nil && !errors.Is(err, nats.ErrBucketNotFound) && !errors.Is(err, nats.ErrStreamNotFound) {
			return fmt.Errorf("failed to delete serving source status store %q, %w", servingSourceStatusStoreKVName, err)
		}
		log.Infow("Succeeded to delete a serving source response store", zap.String("kvName", servingSourceResponseStoreKVName))
	}
	return nil
}

func (jss *jetStreamSvc) ValidateBuffersAndBuckets(ctx context.Context, buffers, buckets []string, sideInputsStore string, servingSourceStore string) error {
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
		otKVName := JetStreamOTKVName(bucket)
		if _, err := jss.js.KeyValue(otKVName); err != nil {
			return fmt.Errorf("failed to query OT KV %q, %w", otKVName, err)
		}

		procKVName := JetStreamProcessorKVName(bucket)
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
	if servingSourceStore != "" {
		servingSourceCallbackStoreKVName := JetStreamServingCallbackStoreName(servingSourceStore)
		if _, err := jss.js.KeyValue(servingSourceCallbackStoreKVName); err != nil {
			return fmt.Errorf("failed to query serving source callback store KV %q, %w", servingSourceCallbackStoreKVName, err)
		}
		servingSourceResponseStoreKVName := JetStreamServingResponseStoreName(servingSourceStore)
		if _, err := jss.js.KeyValue(servingSourceResponseStoreKVName); err != nil {
			return fmt.Errorf("failed to query serving source response store KV %q, %w", servingSourceResponseStoreKVName, err)
		}
		servingSourceStatusStoreKVName := JetStreamServingStatusStoreName(servingSourceStore)
		if _, err := jss.js.KeyValue(servingSourceStatusStoreKVName); err != nil {
			return fmt.Errorf("failed to query serving source status store KV %q, %w", servingSourceStatusStoreKVName, err)
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

func JetStreamName(bufferName string) string {
	return bufferName
}

func JetStreamSideInputsStoreKVName(sideInputStoreName string) string {
	return fmt.Sprintf("%s_SIDE_INPUTS", sideInputStoreName)
}

func JetStreamServingCallbackStoreName(servingSourceStoreName string) string {
	return fmt.Sprintf("%s_SERVING_CALLBACK_STORE", servingSourceStoreName)
}

func JetStreamServingResponseStoreName(servingSourceStoreName string) string {
	return fmt.Sprintf("%s_SERVING_RESPONSE_STORE", servingSourceStoreName)
}

func JetStreamServingStatusStoreName(servingSourceStoreName string) string {
	return fmt.Sprintf("%s_SERVING_STATUS_STORE", servingSourceStoreName)
}
