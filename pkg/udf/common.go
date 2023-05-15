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

package udf

import (
	"context"
	"fmt"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/stores/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
)

func buildRedisBufferIO(ctx context.Context, fromBufferName string, vertexInstance *dfv1.VertexInstance) (isb.BufferReader, map[string]isb.BufferWriter) {
	writers := make(map[string]isb.BufferWriter)
	redisClient := redisclient.NewInClusterRedisClient()
	fromGroup := fromBufferName + "-group"
	readerOpts := []redisclient.Option{}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
		readerOpts = append(readerOpts, redisclient.WithReadTimeOut(x.ReadTimeout.Duration))
	}
	consumer := fmt.Sprintf("%s-%v", vertexInstance.Vertex.Name, vertexInstance.Replica)
	reader := redisisb.NewBufferRead(ctx, redisClient, fromBufferName, fromGroup, consumer, readerOpts...)
	for _, e := range vertexInstance.Vertex.Spec.ToEdges {

		writeOpts := []redisclient.Option{
			redisclient.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
		}
		if x := e.ToVertexLimits; x != nil && x.BufferMaxLength != nil {
			writeOpts = append(writeOpts, redisclient.WithMaxLength(int64(*x.BufferMaxLength)))
		} else if x := e.DeprecatedLimits; x != nil && x.BufferMaxLength != nil {
			// TODO: remove this after deprecation period
			writeOpts = append(writeOpts, redisclient.WithMaxLength(int64(*x.BufferMaxLength)))
		}
		if x := e.ToVertexLimits; x != nil && x.BufferUsageLimit != nil {
			writeOpts = append(writeOpts, redisclient.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
		} else if x := e.DeprecatedLimits; x != nil && x.BufferUsageLimit != nil {
			// TODO: remove this after deprecation period
			writeOpts = append(writeOpts, redisclient.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
		}
		buffers := dfv1.GenerateBufferNames(vertexInstance.Vertex.Namespace, vertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitions())
		for _, buffer := range buffers {
			writer := redisisb.NewBufferWrite(ctx, redisClient, buffer, buffer+"-group", writeOpts...)
			writers[buffer] = writer
		}
	}

	return reader, writers
}

func buildJetStreamBufferIO(ctx context.Context, fromBufferName string, vertexInstance *dfv1.VertexInstance) (isb.BufferReader, map[string]isb.BufferWriter, error) {
	writers := make(map[string]isb.BufferWriter)
	fromStreamName := isbsvc.JetStreamName(fromBufferName)
	readOptions := []jetstreamisb.ReadOption{
		jetstreamisb.WithUsingAckInfoAsRate(true),
	}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
		readOptions = append(readOptions, jetstreamisb.WithReadTimeOut(x.ReadTimeout.Duration))
	}
	reader, err := jetstreamisb.NewJetStreamBufferReader(ctx, jsclient.NewInClusterJetStreamClient(), fromBufferName, fromStreamName, fromStreamName, readOptions...)
	if err != nil {
		return nil, nil, err
	}

	for _, e := range vertexInstance.Vertex.Spec.ToEdges {
		writeOpts := []jetstreamisb.WriteOption{
			jetstreamisb.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
		}
		if x := e.ToVertexLimits; x != nil && x.BufferMaxLength != nil {
			writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
		} else if x := e.DeprecatedLimits; x != nil && x.BufferMaxLength != nil {
			// TODO: remove this after deprecation period
			writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
		}
		if x := e.ToVertexLimits; x != nil && x.BufferUsageLimit != nil {
			writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
		} else if x := e.DeprecatedLimits; x != nil && x.BufferUsageLimit != nil {
			// TODO: remove this after deprecation period
			writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
		}
		buffers := dfv1.GenerateBufferNames(vertexInstance.Vertex.Namespace, vertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitions())
		for _, buffer := range buffers {
			streamName := isbsvc.JetStreamName(buffer)
			writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jsclient.NewInClusterJetStreamClient(), buffer, streamName, streamName, writeOpts...)
			if err != nil {
				return nil, nil, err
			}
			writers[buffer] = writer
		}
	}
	return reader, writers, nil
}
