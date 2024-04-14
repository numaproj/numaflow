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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/stores/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	sinkclient "github.com/numaproj/numaflow/pkg/sdkclient/sinker"
	"github.com/numaproj/numaflow/pkg/sdkserverinfo"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/sinks/blackhole"
	sinkforward "github.com/numaproj/numaflow/pkg/sinks/forward"
	kafkasink "github.com/numaproj/numaflow/pkg/sinks/kafka"
	logsink "github.com/numaproj/numaflow/pkg/sinks/logger"
	"github.com/numaproj/numaflow/pkg/sinks/sinker"
	"github.com/numaproj/numaflow/pkg/sinks/udsink"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store"
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
		sinkHandler        *udsink.UDSgRPCBasedUDSink
		fbSinkHandler      *udsink.UDSgRPCBasedUDSink
		healthCheckers     = make([]metrics.HealthChecker, 0)
	)
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// watermark variables no-op initialization
	// publishWatermark is a map representing a progressor per edge, we are initializing them to a no-op progressor
	// For sinks, the buffer name is the vertex name
	fetchWatermark, _ := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{u.VertexInstance.Vertex.Spec.Name})

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

		natsClientPool, err = jsclient.NewClientPool(ctx, jsclient.WithClientPoolSize(2))
		if err != nil {
			return fmt.Errorf("failed to create a new NATS client pool: %w", err)
		}
		defer natsClientPool.CloseAll()

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
		}

	default:
		return fmt.Errorf("unrecognized isb svc type %q", u.ISBSvcType)
	}
	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, sdkclient.DefaultGRPCMaxMessageSize)
	if udSink := u.VertexInstance.Vertex.Spec.Sink.UDSink; udSink != nil {
		// Wait for server info to be ready
		serverInfo, err := sdkserverinfo.SDKServerInfo(sdkserverinfo.WithServerInfoFilePath(sdkserverinfo.SinkServerInfoFile))
		if err != nil {
			return err
		}

		sdkClient, err := sinkclient.New(serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize))
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

		// add sinkHandler to healthCheckers
		healthCheckers = append(healthCheckers, sinkHandler)

		defer func() {
			err = sdkClient.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close sdk client", zap.Error(err))
			}
		}()
	}

	if u.VertexInstance.Vertex.HasFallbackUDSink() {
		// Wait for server info to be ready
		serverInfo, err := sdkserverinfo.SDKServerInfo(sdkserverinfo.WithServerInfoFilePath(sdkserverinfo.FbSinkServerInfoFile))
		if err != nil {
			return err
		}

		sdkClient, err := sinkclient.New(serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize), sdkclient.WithUdsSockAddr(sdkclient.FbSinkAddr))
		if err != nil {
			return fmt.Errorf("failed to create sdk client, %w", err)
		}

		fbSinkHandler = udsink.NewUDSgRPCBasedUDSink(sdkClient)
		if err != nil {
			return fmt.Errorf("failed to create gRPC client, %w", err)
		}
		// Readiness check
		if err = fbSinkHandler.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on UDSink readiness check, %w", err)
		}

		// add fbSinkHandler to healthCheckers
		healthCheckers = append(healthCheckers, fbSinkHandler)

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

		forwardOpts := []sinkforward.Option{sinkforward.WithLogger(log)}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil {
			if x.ReadBatchSize != nil {
				forwardOpts = append(forwardOpts, sinkforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
			}
		}

		// create the main sink writer
		sinkWriter, err := u.createSinkWriter(ctx, &u.VertexInstance.Vertex.Spec.Sink.AbstractSink, sinkHandler)
		if err != nil {
			return fmt.Errorf("failed to find a sink, error: %w", err)
		}

		// create the fallback sink writer if fallback sink is present
		if u.VertexInstance.Vertex.Spec.Sink.Fallback != nil {
			fbSinkWriter, err := u.createSinkWriter(ctx, u.VertexInstance.Vertex.Spec.Sink.Fallback, fbSinkHandler)
			if err != nil {
				return fmt.Errorf("failed to find a sink, error: %w", err)
			}
			log.Infow("Fallback sink writer created", zap.String("vertex", u.VertexInstance.Vertex.Spec.Sink.Fallback.String()))
			forwardOpts = append(forwardOpts, sinkforward.WithFbSinkWriter(fbSinkWriter))
		}

		// NOTE: forwarder can make multiple sink writers when we introduce fallback sinks
		df, err := sinkforward.NewDataForward(u.VertexInstance, readers[index], sinkWriter, fetchWatermark, forwardOpts...)
		if err != nil {
			return fmt.Errorf("failed to create data forward, error: %w", err)
		}

		// start a forwarder using a goroutine for each partition.
		go func(sinkForwarder forwarder.StarterStopper, fromBufferPartitionName string) {
			defer finalWg.Done()
			log.Infow("Start processing sink messages ", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("fromPartition ", fromBufferPartitionName))
			stopped := sinkForwarder.Start()
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					<-stopped
					log.Info("Sink forwarder stopped, exiting sink processor...")
					return
				}
			}()
			<-ctx.Done()
			log.Infow("SIGTERM exiting inside partition...", zap.String("fromPartition", fromBufferPartitionName))
			sinkForwarder.Stop()
			wg.Wait()
			log.Infow("Exited for partition...", zap.String("fromPartition", fromBufferPartitionName))
		}(df, readers[index].GetName())
	}

	// create lag readers from buffer readers
	var lagReaders []isb.LagReader
	for _, reader := range readers {
		lagReaders = append(lagReaders, reader)
	}

	// start metrics server and pass the sinkHandler to it, so that it can be used to check the readiness of the sink
	metricsOpts := metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, healthCheckers, lagReaders)
	ms := metrics.NewMetricsServer(u.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}

	// wait for all the forwarders to exit
	finalWg.Wait()

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

// createSinkWriter creates a sink writer based on the sink spec
func (u *SinkProcessor) createSinkWriter(ctx context.Context, abstractSink *dfv1.AbstractSink, sinkHandler udsink.SinkApplier) (sinker.SinkWriter, error) {
	if x := abstractSink.Log; x != nil {
		return logsink.NewToLog(ctx, u.VertexInstance)
	} else if x := abstractSink.Kafka; x != nil {
		return kafkasink.NewToKafka(ctx, u.VertexInstance)
	} else if x := abstractSink.Blackhole; x != nil {
		return blackhole.NewBlackhole(ctx, u.VertexInstance)
	} else if x := abstractSink.UDSink; x != nil {
		// if the sink is a user defined sink, then we need to pass the sinkHandler to it which will be used to invoke the user defined sink
		return udsink.NewUserDefinedSink(ctx, u.VertexInstance, sinkHandler)
	}
	return nil, fmt.Errorf("invalid sink spec")
}
