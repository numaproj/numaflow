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
	"os"
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
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	sinkclient "github.com/numaproj/numaflow/pkg/sdkclient/sinker"
	"github.com/numaproj/numaflow/pkg/shared/callback"
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
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
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
		sinkHandlers       []*udsink.UDSgRPCBasedUDSink
		fbSinkHandler      *udsink.UDSgRPCBasedUDSink
		healthCheckers     = make([]metrics.HealthChecker, 0)
		vertexName         = u.VertexInstance.Vertex.Spec.Name
		pipelineName       = u.VertexInstance.Vertex.Spec.PipelineName
	)
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// watermark variables no-op initialization
	// publishWatermark is a map representing a progressor per edge, we are initializing them to a no-op progressor
	// For sinks, the buffer name is the vertex name
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList([]string{vertexName})
	idleManager = wmb.NewNoOpIdleManager()

	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		redisClient := redisclient.NewInClusterRedisClient()
		var readOptions []redisclient.Option
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

			// create watermark stores
			sinkWmStores, err = jetstream.BuildToVertexWatermarkStores(ctx, u.VertexInstance, natsClientPool.NextAvailableClient())
			if err != nil {
				return fmt.Errorf("failed to to vertex watermark stores: %w", err)
			}

			// create watermark publisher using watermark stores
			publishWatermark = jetstream.BuildPublishersFromStores(ctx, u.VertexInstance, sinkWmStores)
			// sink vertex has only one toBuffer, so the length is 1
			idleManager, _ = wmb.NewIdleManager(len(readers), 1)
		}

	default:
		return fmt.Errorf("unrecognized isb svc type %q", u.ISBSvcType)
	}
	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, sdkclient.DefaultGRPCMaxMessageSize)
	if udSink := u.VertexInstance.Vertex.Spec.Sink.UDSink; udSink != nil {
		// Wait for server info to be ready
		serverInfo, err := serverinfo.SDKServerInfo(serverinfo.WithServerInfoFilePath(sdkclient.SinkServerInfoFile))
		if err != nil {
			return err
		}
		metrics.SDKInfo.WithLabelValues(dfv1.ComponentVertex, fmt.Sprintf("%s-%s", pipelineName, vertexName), string(serverinfo.ContainerTypeSinker), serverInfo.Version, string(serverInfo.Language)).Set(1)

		// we should create a new sdk client for each partition, because each client is mapped to one bidirectional stream
		// and the bidi stream cannot be shared.
		for index := range u.VertexInstance.Vertex.OwnedBuffers() {
			sdkClient, err := sinkclient.New(ctx, serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize))
			if err != nil {
				return fmt.Errorf("failed to create sdk client, %w", err)
			}

			sinkHandler := udsink.NewUDSgRPCBasedUDSink(sdkClient)
			// Readiness check
			if err := sinkHandler.WaitUntilReady(ctx); err != nil {
				return fmt.Errorf("failed on UDSink readiness check, %w", err)
			}

			// since it is the same UD Container, we need to do health check only once.
			if index == 0 {
				healthCheckers = append(healthCheckers, sinkHandler)
			}

			sinkHandlers = append(sinkHandlers, sinkHandler)

			defer func() {
				err = sdkClient.CloseConn(ctx)
				if err != nil {
					log.Warnw("Failed to close sdk client", zap.Error(err))
				}
			}()
		}
	}

	if u.VertexInstance.Vertex.HasFallbackUDSink() {
		// Wait for server info to be ready
		serverInfo, err := serverinfo.SDKServerInfo(serverinfo.WithServerInfoFilePath(sdkclient.FbSinkServerInfoFile))
		if err != nil {
			return err
		}
		metrics.SDKInfo.WithLabelValues(dfv1.ComponentVertex, fmt.Sprintf("%s-%s", pipelineName, vertexName), string(serverinfo.ContainerTypeFbsinker), serverInfo.Version, string(serverInfo.Language)).Set(1)

		sdkClient, err := sinkclient.New(ctx, serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize), sdkclient.WithUdsSockAddr(sdkclient.FbSinkAddr))
		if err != nil {
			return fmt.Errorf("failed to create sdk client, %w", err)
		}

		fbSinkHandler = udsink.NewUDSgRPCBasedUDSink(sdkClient)
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

		// we need to use separate sink handlers for each partition, because each sink handler is mapped to one bidirectional stream
		// and the bidi stream cannot be shared between partitions.
		var udsinkHandler *udsink.UDSgRPCBasedUDSink
		if len(sinkHandlers) > 0 {
			udsinkHandler = sinkHandlers[index]
		}

		// create the main sink writer
		sinkWriter, err := u.createSinkWriter(ctx, &u.VertexInstance.Vertex.Spec.Sink.AbstractSink, udsinkHandler)
		if err != nil {
			return fmt.Errorf("failed to find a sink, error: %w", err)
		}

		// create the fallback sink writer if fallback sink is present
		fbSink := u.VertexInstance.Vertex.Spec.Sink.Fallback
		if fbSink != nil && fbSink.IsAnySinkSpecified() {
			fbSinkWriter, err := u.createSinkWriter(ctx, u.VertexInstance.Vertex.Spec.Sink.Fallback, fbSinkHandler)
			if err != nil {
				return fmt.Errorf("failed to find a sink, error: %w", err)
			}
			log.Info("Fallback sink writer created")
			forwardOpts = append(forwardOpts, sinkforward.WithFbSinkWriter(fbSinkWriter))
		}

		// if the callback is enabled, create a callback publisher
		cbEnabled := sharedutil.LookupEnvBoolOr(dfv1.EnvCallbackEnabled, false)
		if cbEnabled {
			cbOpts := make([]callback.OptionFunc, 0)
			cbUrl := os.Getenv(dfv1.EnvCallbackURL)
			if cbUrl != "" {
				cbOpts = append(cbOpts, callback.WithCallbackURL(cbUrl))
			}
			cbPublisher := callback.NewUploader(ctx, vertexName, pipelineName, cbOpts...)
			forwardOpts = append(forwardOpts, sinkforward.WithCallbackUploader(cbPublisher))
		}

		df, err := sinkforward.NewDataForward(u.VertexInstance, readers[index], sinkWriter, fetchWatermark, publishWatermark[vertexName], idleManager, forwardOpts...)
		if err != nil {
			return fmt.Errorf("failed to create data forward, error: %w", err)
		}

		// start a forwarder using a goroutine for each partition.
		go func(sinkForwarder forwarder.StarterStopper, fromBufferPartitionName string) {
			defer finalWg.Done()
			log.Infow("Start processing sink messages ", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("fromPartition ", fromBufferPartitionName))
			stopped := sinkForwarder.Start()
			select {
			case <-ctx.Done(): // context cancelled case
				log.Info("Context cancelled, stopping forwarder for partition...", zap.String("partition", fromBufferPartitionName))
				sinkForwarder.Stop()
				if err := <-stopped; err != nil {
					log.Errorw("Sink forwarder stopped with error", zap.String("fromPartition", fromBufferPartitionName), zap.Error(err))
				}
				log.Info("Exited for partition...", zap.String("partition", fromBufferPartitionName))
			case err := <-stopped: // critical error case
				if err != nil {
					log.Errorw("Sink forwarder stopped with error", zap.String("fromPartition", fromBufferPartitionName), zap.Error(err))
					cancel()
				}
			}
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

	// close the fromVertex wm stores
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
		// if the sink is a user-defined sink, then we need to pass the sinkHandler to it which will be used to invoke the user-defined sink
		return udsink.NewUserDefinedSink(ctx, u.VertexInstance, sinkHandler)
	}
	return nil, fmt.Errorf("invalid sink spec")
}
