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

	"github.com/numaproj/numaflow/pkg/sdkclient"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/stores/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/metrics"
	sinkclient "github.com/numaproj/numaflow/pkg/sdkclient/sinker"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sinks/blackhole"
	kafkasink "github.com/numaproj/numaflow/pkg/sinks/kafka"
	logsink "github.com/numaproj/numaflow/pkg/sinks/logger"
	"github.com/numaproj/numaflow/pkg/sinks/udsink"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

type SinkProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *SinkProcessor) Start(ctx context.Context) error {
	var (
		readers            []isb.BufferReader
		natsClientPool     *jsclient.ClientPool
		err                error
		fromVertexWmStores map[string]store.WatermarkStore
		sinkWmStores       map[string]store.WatermarkStore
		idleManager        wmb.IdleManager
		sdkClient          sinkclient.Client
		sinkHandler        *udsink.UDSgRPCBasedUDSink
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
	idleManager = wmb.NewNoOpIdleManager()

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

		// create reader for each partition. Each partition is a stream in jetstream
		for index, bufferPartition := range u.VertexInstance.Vertex.OwnedBuffers() {
			fromStreamName := isbsvc.JetStreamName(bufferPartition)

			reader, err := jetstreamisb.NewJetStreamBufferReader(ctx, natsClientPool.NextAvailableClient(), bufferPartition, fromStreamName, fromStreamName, int32(index), readOptions...)
			if err != nil {
				return err
			}
			readers = append(readers, reader)
		}

		if u.VertexInstance.Vertex.Spec.Watermark.Disabled {
			// use default no op fetcher, publisher, idleManager
		} else {
			// build from vertex watermark stores
			fromVertexWmStores, err = jetstream.BuildFromVertexWatermarkStores(ctx, u.VertexInstance, natsClientPool.NextAvailableClient())
			if err != nil {
				return fmt.Errorf("failed to from vertex watermark stores: %w", err)
			}

			// create watermark fetcher using watermark stores
			fetchWatermark = fetch.NewEdgeFetcherSet(ctx, u.VertexInstance, fromVertexWmStores)

			// create watermark stores
			sinkWmStores, err = jetstream.BuildToVertexWatermarkStores(ctx, u.VertexInstance, natsClientPool.NextAvailableClient())
			if err != nil {
				return fmt.Errorf("failed to to vertex watermark stores: %w", err)
			}

			// create watermark publisher using watermark stores
			publishWatermark = jetstream.BuildPublishersFromStores(ctx, u.VertexInstance, sinkWmStores)
			// sink vertex has only one toBuffer, so the length is 1
			idleManager = wmb.NewIdleManager(1)
		}

	default:
		return fmt.Errorf("unrecognized isb svc type %q", u.ISBSvcType)
	}
	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, sdkclient.DefaultGRPCMaxMessageSize)
	if udSink := u.VertexInstance.Vertex.Spec.Sink.UDSink; udSink != nil {
		sdkClient, err = sinkclient.New(sinkclient.WithMaxMessageSize(maxMessageSize))
		if err != nil {
			return fmt.Errorf("failed to create sdk client, %w", err)
		}

		sinkHandler = udsink.NewUDSgRPCBasedUDSink(sdkClient)
		if err != nil {
			return fmt.Errorf("failed to create gRPC client, %w", err)
		}
		// Readiness check
		if err := sinkHandler.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on UDSink readiness check, %w", err)
		}

		defer func() {
			err = sdkClient.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close sdk client", zap.Error(err))
			}
		}()
	}

	var finalWg sync.WaitGroup
	for index := range u.VertexInstance.Vertex.OwnedBuffers() {
		finalWg.Add(1)
		sinker, err := u.getSinker(readers[index], log, fetchWatermark, publishWatermark[u.VertexInstance.Vertex.Spec.Name], idleManager, sinkHandler)
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
			log.Infow("SIGTERM exiting inside partition...", zap.String("fromPartition", fromBufferPartitionName))
			sinker.Stop()
			wg.Wait()
			log.Infow("Exited for partition...", zap.String("fromPartition", fromBufferPartitionName))
		}(sinker, readers[index].GetName())
	}

	// create lag readers from buffer readers
	var lagReaders []isb.LagReader
	for _, reader := range readers {
		lagReaders = append(lagReaders, reader)
	}
	// start metrics server and pass the sinkHandler to it, so that it can be used to check the readiness of the sink
	metricsOpts := metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, []metrics.HealthChecker{sinkHandler}, lagReaders)
	ms := metrics.NewMetricsServer(u.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}

	// wait for all the sinkers to exit
	finalWg.Wait()

	// closing the publisher will only delete the keys from the store, but not the store itself
	// we cannot close the store inside publisher because in some cases stores are shared between publishers
	// and store itself is a separate entity that can be used by other components
	for _, publisher := range publishWatermark {
		err = publisher.Close()
		if err != nil {
			log.Errorw("Failed to close the watermark publisher", zap.Error(err))
		}
	}

	// close the from vertex wm stores
	// since we created the stores, we can close them
	for _, wmStore := range fromVertexWmStores {
		_ = wmStore.Close()
	}

	// close the sink wm stores
	// since we created the stores, we can close them
	for _, wmStore := range sinkWmStores {
		_ = wmStore.Close()
	}

	log.Info("Exited...")
	return nil
}

// getSinker takes in the logger from the parent context
func (u *SinkProcessor) getSinker(reader isb.BufferReader, logger *zap.SugaredLogger, fetchWM fetch.Fetcher, publishWM publish.Publisher, idleManager wmb.IdleManager, sinkHandler udsink.SinkApplier) (Sinker, error) {
	sink := u.VertexInstance.Vertex.Spec.Sink
	if x := sink.Log; x != nil {
		return logsink.NewToLog(u.VertexInstance, reader, fetchWM, publishWM, idleManager, logsink.WithLogger(logger))
	} else if x := sink.Kafka; x != nil {
		return kafkasink.NewToKafka(u.VertexInstance, reader, fetchWM, publishWM, idleManager, kafkasink.WithLogger(logger))
	} else if x := sink.Blackhole; x != nil {
		return blackhole.NewBlackhole(u.VertexInstance, reader, fetchWM, publishWM, idleManager, blackhole.WithLogger(logger))
	} else if x := sink.UDSink; x != nil {
		// if the sink is a user defined sink, then we need to pass the sinkHandler to it which will be used to invoke the user defined sink
		return udsink.NewUserDefinedSink(u.VertexInstance, reader, fetchWM, publishWM, idleManager, sinkHandler, udsink.WithLogger(logger))
	}
	return nil, fmt.Errorf("invalid sink spec")
}
