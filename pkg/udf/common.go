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

func buildRedisBufferIO(ctx context.Context, vertexInstance *dfv1.VertexInstance) ([]isb.PartitionReader, map[string][]isb.PartitionWriter, error) {
	var readers []isb.PartitionReader
	redisClient := redisclient.NewInClusterRedisClient()
	var readerOpts []redisclient.Option
	if x := vertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
		readerOpts = append(readerOpts, redisclient.WithReadTimeOut(x.ReadTimeout.Duration))
	}
	// create readers for owned buffer partitions.
	// For reduce vertex, we only need to read from one buffer partition.
	if vertexInstance.Vertex.GetVertexType() == dfv1.VertexTypeReduceUDF {
		var fromBufferPartition string
		// find the buffer partition owned by this replica.
		for _, b := range vertexInstance.Vertex.OwnedPartitions() {
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
		// since we read from one buffer partition, fromPartitionIdx is 0.
		reader := redisisb.NewPartitionReader(ctx, redisClient, fromBufferPartition, fromGroup, consumer, 0, readerOpts...)
		readers = append(readers, reader)
	} else {
		// for map vertex, we need to read from all buffer partitions. So create readers for all buffer partitions.
		for _, bufferPartition := range vertexInstance.Vertex.OwnedPartitions() {
			fromGroup := bufferPartition + "-group"
			consumer := fmt.Sprintf("%s-%v", vertexInstance.Vertex.Name, vertexInstance.Replica)
			reader := redisisb.NewPartitionReader(ctx, redisClient, bufferPartition, fromGroup, consumer, 0, readerOpts...)
			readers = append(readers, reader)
		}
	}

	// create writers for toVertex's buffer partitions.
	// we create a map of toVertex -> []PartitionWriter(writer for each partition)
	writers := make(map[string][]isb.PartitionWriter)
	for _, e := range vertexInstance.Vertex.Spec.ToEdges {

		writeOpts := []redisclient.Option{
			redisclient.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
		}
		if x := e.ToVertexLimits; x != nil && x.PartitionMaxLength != nil {
			writeOpts = append(writeOpts, redisclient.WithMaxLength(int64(*x.PartitionMaxLength)))
		} else if x := e.DeprecatedLimits; x != nil && x.BufferMaxLength != nil {
			// TODO: remove this after deprecation period
			writeOpts = append(writeOpts, redisclient.WithMaxLength(int64(*x.BufferMaxLength)))
		}
		if x := e.ToVertexLimits; x != nil && x.PartitionUsageLimit != nil {
			writeOpts = append(writeOpts, redisclient.WithBufferUsageLimit(float64(*x.PartitionUsageLimit)/100))
		} else if x := e.DeprecatedLimits; x != nil && x.BufferUsageLimit != nil {
			// TODO: remove this after deprecation period
			writeOpts = append(writeOpts, redisclient.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
		}
		var edgeBuffers []isb.PartitionWriter
		buffers := dfv1.GeneratePartitionNames(vertexInstance.Vertex.Namespace, vertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitionCount())
		for _, buffer := range buffers {
			writer := redisisb.NewPartitionWrite(ctx, redisClient, buffer, buffer+"-group", writeOpts...)
			edgeBuffers = append(edgeBuffers, writer)
		}
		writers[e.To] = edgeBuffers
	}

	return readers, writers, nil
}

func buildJetStreamBufferIO(ctx context.Context, vertexInstance *dfv1.VertexInstance) ([]isb.PartitionReader, map[string][]isb.PartitionWriter, error) {

	// create readers for owned buffer partitions.
	var readers []isb.PartitionReader
	var readOptions []jetstreamisb.ReadOption
	if x := vertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
		readOptions = append(readOptions, jetstreamisb.WithReadTimeOut(x.ReadTimeout.Duration))
	}

	// create readers for owned buffer partitions.
	// For reduce vertex, we only need to read from one buffer partition.
	if vertexInstance.Vertex.GetVertexType() == dfv1.VertexTypeReduceUDF {
		// choose the buffer that corresponds to this reduce processor because
		// reducer's incoming buffer can have more than one partition for parallelism
		var fromBufferPartition string
		// find the buffer partition owned by this replica.
		for _, b := range vertexInstance.Vertex.OwnedPartitions() {
			if strings.HasSuffix(b, fmt.Sprintf("-%d", vertexInstance.Replica)) {
				fromBufferPartition = b
				break
			}
		}
		if len(fromBufferPartition) == 0 {
			return nil, nil, fmt.Errorf("can not find from buffer")
		}

		fromStreamName := isbsvc.JetStreamName(fromBufferPartition)
		// reduce processor only has one buffer partition
		// since we read from one buffer partition, fromPartitionIdx is 0.
		reader, err := jetstreamisb.NewJetStreamReader(ctx, jsclient.NewInClusterJetStreamClient(), fromBufferPartition, fromStreamName, fromStreamName, 0, readOptions...)
		if err != nil {
			return nil, nil, err
		}
		readers = append(readers, reader)
	} else {
		// for map vertex, we need to read from all buffer partitions. So create readers for all buffer partitions.
		for index, bufferPartition := range vertexInstance.Vertex.OwnedPartitions() {
			fromStreamName := isbsvc.JetStreamName(bufferPartition)

			reader, err := jetstreamisb.NewJetStreamReader(ctx, jsclient.NewInClusterJetStreamClient(), bufferPartition, fromStreamName, fromStreamName, int32(index), readOptions...)
			if err != nil {
				return nil, nil, err
			}
			readers = append(readers, reader)
		}
	}

	// create writers for toVertex's buffer partitions.
	// we create a map of toVertex -> []PartitionWriter(writer for each partition)
	writers := make(map[string][]isb.PartitionWriter)
	for _, e := range vertexInstance.Vertex.Spec.ToEdges {
		writeOpts := []jetstreamisb.WriteOption{
			jetstreamisb.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
		}
		if x := e.ToVertexLimits; x != nil && x.PartitionMaxLength != nil {
			writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.PartitionMaxLength)))
		} else if x := e.DeprecatedLimits; x != nil && x.BufferMaxLength != nil {
			// TODO: remove this after deprecation period
			writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
		}
		if x := e.ToVertexLimits; x != nil && x.PartitionUsageLimit != nil {
			writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.PartitionUsageLimit)/100))
		} else if x := e.DeprecatedLimits; x != nil && x.BufferUsageLimit != nil {
			// TODO: remove this after deprecation period
			writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
		}

		buffers := dfv1.GeneratePartitionNames(vertexInstance.Vertex.Namespace, vertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitionCount())
		var edgeBuffers []isb.PartitionWriter
		for _, buffer := range buffers {
			streamName := isbsvc.JetStreamName(buffer)
			writer, err := jetstreamisb.NewJetStreamWriter(ctx, jsclient.NewInClusterJetStreamClient(), buffer, streamName, streamName, writeOpts...)
			if err != nil {
				return nil, nil, err
			}
			edgeBuffers = append(edgeBuffers, writer)
		}

		writers[e.To] = edgeBuffers
	}
	return readers, writers, nil
}
