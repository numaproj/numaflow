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

package flatmap

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
	// For reduce vertex, we only need to read from one buffer partition.
	if vertexInstance.Vertex.GetVertexType() == dfv1.VertexTypeReduceUDF {
		var fromBufferPartition string
		// find the buffer partition owned by this replica.
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
		// since we read from one buffer partition, fromPartitionIdx is 0.
		reader := redisisb.NewBufferRead(ctx, redisClient, fromBufferPartition, fromGroup, consumer, 0, readerOpts...)
		readers = append(readers, reader)
	} else {
		// for map vertex, we need to read from all buffer partitions. So create readers for all buffer partitions.
		for _, bufferPartition := range vertexInstance.Vertex.OwnedBuffers() {
			fromGroup := bufferPartition + "-group"
			consumer := fmt.Sprintf("%s-%v", vertexInstance.Vertex.Name, vertexInstance.Replica)
			reader := redisisb.NewBufferRead(ctx, redisClient, bufferPartition, fromGroup, consumer, 0, readerOpts...)
			readers = append(readers, reader)
		}
	}

	// create writers for toVertex's buffer partitions.
	// we create a map of toVertex -> []BufferWriter(writer for each partition)
	writers := make(map[string][]isb.BufferWriter)
	for _, e := range vertexInstance.Vertex.Spec.ToEdges {

		writeOpts := []redisclient.Option{
			redisclient.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
		}
		if x := e.ToVertexLimits; x != nil && x.BufferMaxLength != nil {
			writeOpts = append(writeOpts, redisclient.WithMaxLength(int64(*x.BufferMaxLength)))
		}
		if x := e.ToVertexLimits; x != nil && x.BufferUsageLimit != nil {
			writeOpts = append(writeOpts, redisclient.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
		}
		var edgeBuffers []isb.BufferWriter
		partitionedBuffers := dfv1.GenerateBufferNames(vertexInstance.Vertex.Namespace, vertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitionCount())
		for partitionIdx, partition := range partitionedBuffers {
			writer := redisisb.NewBufferWrite(ctx, redisClient, partition, partition+"-group", int32(partitionIdx), writeOpts...)
			edgeBuffers = append(edgeBuffers, writer)
		}
		writers[e.To] = edgeBuffers
	}

	return readers, writers, nil
}

func buildJetStreamBufferIO(ctx context.Context, vertexInstance *dfv1.VertexInstance, clientPool *jsclient.ClientPool) ([]isb.BufferReader, map[string][]isb.BufferWriter, error) {

	// create readers for owned buffer partitions.
	var readers []isb.BufferReader
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
		// reduce processor only has one buffer partition
		// since we read from one buffer partition, fromPartitionIdx is 0.
		reader, err := jetstreamisb.NewJetStreamBufferReader(ctx, clientPool.NextAvailableClient(), fromBufferPartition, fromStreamName, fromStreamName, 0, readOptions...)
		if err != nil {
			return nil, nil, err
		}
		readers = append(readers, reader)
	} else {
		// for map vertex, we need to read from all buffer partitions. So create readers for all buffer partitions.
		for index, bufferPartition := range vertexInstance.Vertex.OwnedBuffers() {
			fromStreamName := isbsvc.JetStreamName(bufferPartition)

			reader, err := jetstreamisb.NewJetStreamBufferReader(ctx, clientPool.NextAvailableClient(), bufferPartition, fromStreamName, fromStreamName, int32(index), readOptions...)
			if err != nil {
				return nil, nil, err
			}
			readers = append(readers, reader)
		}
	}

	// create writers for toVertex's buffer partitions.
	// we create a map of toVertex -> []BufferWriter(writer for each partition)
	writers := make(map[string][]isb.BufferWriter)
	for _, e := range vertexInstance.Vertex.Spec.ToEdges {
		writeOpts := []jetstreamisb.WriteOption{
			jetstreamisb.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
		}
		if x := e.ToVertexLimits; x != nil && x.BufferMaxLength != nil {
			writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
		}
		if x := e.ToVertexLimits; x != nil && x.BufferUsageLimit != nil {
			writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
		}

		partitionedBuffers := dfv1.GenerateBufferNames(vertexInstance.Vertex.Namespace, vertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitionCount())
		var edgeBuffers []isb.BufferWriter
		for partitionIdx, partition := range partitionedBuffers {
			streamName := isbsvc.JetStreamName(partition)
			writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, clientPool.NextAvailableClient(), partition, streamName, streamName, int32(partitionIdx), writeOpts...)
			if err != nil {
				return nil, nil, err
			}
			edgeBuffers = append(edgeBuffers, writer)
		}

		writers[e.To] = edgeBuffers
	}
	return readers, writers, nil
}
