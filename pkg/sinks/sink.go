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

package sinks

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/forward"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/stores/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/metrics"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sinks/blackhole"
	kafkasink "github.com/numaproj/numaflow/pkg/sinks/kafka"
	logsink "github.com/numaproj/numaflow/pkg/sinks/logger"
	"github.com/numaproj/numaflow/pkg/sinks/udsink"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

type SinkProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *SinkProcessor) Start(ctx context.Context) error {
	var (
		readers        []isb.BufferReader
		natsClientPool *jsclient.ClientPool
		err            error
	)
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	natsClientPool, err = jsclient.NewClientPool(ctx, jsclient.WithClientPoolSize(2))
	if err != nil {
		return fmt.Errorf("failed to create a new NATS client pool: %w", err)
	}
	defer natsClientPool.CloseAll()
	// watermark variables no-op initialization
	// publishWatermark is a map representing a progressor per edge, we are initializing them to a no-op progressor
	// For sinks, the buffer name is the vertex name
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{u.VertexInstance.Vertex.Spec.Name})

	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		redisClient := redisclient.NewInClusterRedisClient()
		readOptions := []redisclient.Option{}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
			readOptions = append(readOptions, redisclient.WithReadTimeOut(x.ReadTimeout.Duration))
		}
		// create reader for each partition. Each partition is a group in redis
		for index, bufferPartition := range u.VertexInstance.Vertex.OwnedBuffers() {
			fromGroup := bufferPartition + "-group"
			consumer := fmt.Sprintf("%s-%v", u.VertexInstance.Vertex.Name, u.VertexInstance.Replica)

			reader := redisisb.NewBufferRead(ctx, redisClient, bufferPartition, fromGroup, consumer, int32(index), readOptions...)
			readers = append(readers, reader)
		}
	case dfv1.ISBSvcTypeJetStream:
		var readOptions []jetstreamisb.ReadOption
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
			readOptions = append(readOptions, jetstreamisb.WithReadTimeOut(x.ReadTimeout.Duration))
		}
		// build watermark progressors
		fetchWatermark, publishWatermark, err = jetstream.BuildWatermarkProgressors(ctx, u.VertexInstance, natsClientPool.NextAvailableClient())
		if err != nil {
			return err
		}

		// create reader for each partition. Each partition is a stream in jetstream
		for index, bufferPartition := range u.VertexInstance.Vertex.OwnedBuffers() {
			fromStreamName := isbsvc.JetStreamName(bufferPartition)

			reader, err := jetstreamisb.NewJetStreamBufferReader(ctx, natsClientPool.NextAvailableClient(), bufferPartition, fromStreamName, fromStreamName, int32(index), readOptions...)
			if err != nil {
				return err
			}
			readers = append(readers, reader)
		}
	default:
		return fmt.Errorf("unrecognized isb svc type %q", u.ISBSvcType)
	}

	var sinkHandler *udsink.UDSgRPCBasedUDSink = nil
	if udSink := u.VertexInstance.Vertex.Spec.Sink.UDSink; udSink != nil {
		sinkHandler, err = udsink.NewUDSgRPCBasedUDSink()
		if err != nil {
			return fmt.Errorf("failed to create gRPC client, %w", err)
		}
		// Readiness check
		if err := sinkHandler.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on UDSink readiness check, %w", err)
		}
		// close the connection when the function exits
		defer func() {
			err = sinkHandler.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close gRPC client conn", zap.Error(err))
			}
		}()
	}

	var finalWg sync.WaitGroup
	for index := range u.VertexInstance.Vertex.OwnedBuffers() {
		finalWg.Add(1)
		sinker, err := u.getSinker(readers[index], log, fetchWatermark, publishWatermark, sinkHandler)
		if err != nil {
			return fmt.Errorf("failed to find a sink, errpr: %w", err)
		}
		// start sinker using a goroutine for each partition.
		go func(sinker Sinker, fromBufferPartitionName string) {
			defer finalWg.Done()
			log.Infow("Start processing sink messages ", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("fromPartition ", fromBufferPartitionName))
			stopped := sinker.Start()
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					<-stopped
					log.Info("Sinker stopped, exiting sink processor...")
					return
				}
			}()
			<-ctx.Done()
			log.Info("SIGTERM exiting inside partition...", zap.String("fromPartition", fromBufferPartitionName))
			sinker.Stop()
			wg.Wait()
			log.Info("Exited for partition...", zap.String("fromPartition", fromBufferPartitionName))
		}(sinker, readers[index].GetName())
	}
	// start metrics server and pass the sinkHandler to it, so that it can be used to check the readiness of the sink
	metricsOpts := metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, []metrics.HealthChecker{sinkHandler}, readers)
	ms := metrics.NewMetricsServer(u.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}

	// wait for all the sinkers to exit
	finalWg.Wait()
	log.Info("All udsink processors exited...")
	return nil
}

// getSinker takes in the logger from the parent context
func (u *SinkProcessor) getSinker(reader isb.BufferReader, logger *zap.SugaredLogger, fetchWM fetch.Fetcher, publishWM map[string]publish.Publisher, sinkHandler *udsink.UDSgRPCBasedUDSink) (Sinker, error) {
	sink := u.VertexInstance.Vertex.Spec.Sink
	if x := sink.Log; x != nil {
		return logsink.NewToLog(u.VertexInstance.Vertex, reader, fetchWM, publishWM, u.getSinkGoWhereDecider(), logsink.WithLogger(logger))
	} else if x := sink.Kafka; x != nil {
		return kafkasink.NewToKafka(u.VertexInstance.Vertex, reader, fetchWM, publishWM, u.getSinkGoWhereDecider(), kafkasink.WithLogger(logger))
	} else if x := sink.Blackhole; x != nil {
		return blackhole.NewBlackhole(u.VertexInstance.Vertex, reader, fetchWM, publishWM, u.getSinkGoWhereDecider(), blackhole.WithLogger(logger))
	} else if x := sink.UDSink; x != nil {
		// if the sink is a user defined sink, then we need to pass the sinkHandler to it which will be used to invoke the user defined sink
		return udsink.NewUserDefinedSink(u.VertexInstance.Vertex, reader, fetchWM, publishWM, u.getSinkGoWhereDecider(), sinkHandler, udsink.WithLogger(logger))
	}
	return nil, fmt.Errorf("invalid sink spec")
}

// getSinkGoWhereDecider returns a function that decides where to send the message
// based on the keys and tags
// for sink processor, we send the message to the same vertex and partition will be set to 0
func (u *SinkProcessor) getSinkGoWhereDecider() forward.GoWhere {
	fsd := forward.GoWhere(func(keys []string, tags []string) ([]forward.VertexBuffer, error) {
		var result []forward.VertexBuffer
		result = append(result, forward.VertexBuffer{
			ToVertexName:         u.VertexInstance.Vertex.Spec.Name,
			ToVertexPartitionIdx: 0,
		})
		return result, nil
	})
	return fsd
}
