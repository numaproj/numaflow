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
	"strings"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/stores/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
)

func buildRedisBufferIO(ctx context.Context, vertexInstance *dfv1.VertexInstance) ([]isb.BufferReader, map[string][]isb.BufferWriter, error) {
	var readers []isb.BufferReader
	redisClient := redisclient.NewInClusterRedisClient()
	var readerOpts []redisclient.Option
	if x := vertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
		readerOpts = append(readerOpts, redisclient.WithReadTimeOut(x.ReadTimeout.Duration))
	}
	// create readers for owned buffer partitions.
	if vertexInstance.Vertex.GetVertexType() == dfv1.VertexTypeReduceUDF {
		var fromBufferPartition string
		for _, b := range vertexInstance.Vertex.OwnedBuffers() {
			if strings.HasSuffix(b, fmt.Sprintf("-%d", vertexInstance.Replica)) {
				fromBufferPartition = b
				break
			}
		}
		if len(fromBufferPartition) == 0 {
			return nil, nil, fmt.Errorf("can not find from buffer")
		}

		fromGroup := fromBufferPartition + "-group"
		consumer := fmt.Sprintf("%s-%v", vertexInstance.Vertex.Name, vertexInstance.Replica)
		reader := redisisb.NewBufferRead(ctx, redisClient, fromBufferPartition, fromGroup, consumer, readerOpts...)
		readers = append(readers, reader)
	} else {
		for _, bufferPartition := range vertexInstance.Vertex.OwnedBuffers() {
			fromGroup := bufferPartition + "-group"
			consumer := fmt.Sprintf("%s-%v", vertexInstance.Vertex.Name, vertexInstance.Replica)
			reader := redisisb.NewBufferRead(ctx, redisClient, bufferPartition, fromGroup, consumer, readerOpts...)
			readers = append(readers, reader)
		}
	}

	// create writers for toVertex's buffer partitions.
	writers := make(map[string][]isb.BufferWriter)
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
		// TODO: support multiple partitions
		var edgeBuffers []isb.BufferWriter
		buffers := dfv1.GenerateBufferNames(vertexInstance.Vertex.Namespace, vertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitions())
		if e.ToVertexType == dfv1.VertexTypeReduceUDF {
			for _, buffer := range buffers {
				writer := redisisb.NewBufferWrite(ctx, redisClient, buffer, buffer+"-group", writeOpts...)
				edgeBuffers = append(edgeBuffers, writer)
			}
		} else {
			writer := redisisb.NewBufferWrite(ctx, redisClient, buffers[0], buffers[0]+"-group", writeOpts...)
			edgeBuffers = append(edgeBuffers, writer)
		}
		writers[e.To] = edgeBuffers
	}

	return readers, writers, nil
}

func buildJetStreamBufferIO(ctx context.Context, vertexInstance *dfv1.VertexInstance) ([]isb.BufferReader, map[string][]isb.BufferWriter, error) {

	// create readers for owned buffer partitions.
	var readers []isb.BufferReader
	readOptions := []jetstreamisb.ReadOption{
		jetstreamisb.WithUsingAckInfoAsRate(true),
	}
	if x := vertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
		readOptions = append(readOptions, jetstreamisb.WithReadTimeOut(x.ReadTimeout.Duration))
	}

	if vertexInstance.Vertex.GetVertexType() == dfv1.VertexTypeReduceUDF {
		// choose the buffer that corresponds to this reduce processor because
		// reducer's incoming buffer can have more than one partition for parallelism
		var fromBufferPartition string
		for _, b := range vertexInstance.Vertex.OwnedBuffers() {
			if strings.HasSuffix(b, fmt.Sprintf("-%d", vertexInstance.Replica)) {
				fromBufferPartition = b
				break
			}
		}
		if len(fromBufferPartition) == 0 {
			return nil, nil, fmt.Errorf("can not find from buffer")
		}
		
		fromStreamName := isbsvc.JetStreamName(fromBufferPartition)
		reader, err := jetstreamisb.NewJetStreamBufferReader(ctx, jsclient.NewInClusterJetStreamClient(), fromBufferPartition, fromStreamName, fromStreamName, readOptions...)
		if err != nil {
			return nil, nil, err
		}
		readers = append(readers, reader)
	} else {
		for _, bufferPartition := range vertexInstance.Vertex.OwnedBuffers() {
			fromStreamName := isbsvc.JetStreamName(bufferPartition)

			reader, err := jetstreamisb.NewJetStreamBufferReader(ctx, jsclient.NewInClusterJetStreamClient(), bufferPartition, fromStreamName, fromStreamName, readOptions...)
			if err != nil {
				return nil, nil, err
			}
			readers = append(readers, reader)
		}
	}

	// create writers for toVertex's buffer partitions
	writers := make(map[string][]isb.BufferWriter)
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
		// TODO: support multiple partitions
		buffers := dfv1.GenerateBufferNames(vertexInstance.Vertex.Namespace, vertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitions())
		var edgeBuffers []isb.BufferWriter
		if e.ToVertexType == dfv1.VertexTypeReduceUDF {
			for _, buffer := range buffers {
				streamName := isbsvc.JetStreamName(buffer)
				writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jsclient.NewInClusterJetStreamClient(), buffer, streamName, streamName, writeOpts...)
				if err != nil {
					return nil, nil, err
				}
				edgeBuffers = append(edgeBuffers, writer)
			}
		} else {
			streamName := isbsvc.JetStreamName(buffers[0])
			writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jsclient.NewInClusterJetStreamClient(), buffers[0], streamName, streamName, writeOpts...)
			if err != nil {
				return nil, nil, err
			}
			edgeBuffers = append(edgeBuffers, writer)
		}
		writers[e.To] = edgeBuffers
	}
	return readers, writers, nil
}
