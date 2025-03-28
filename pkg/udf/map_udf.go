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
	"io"
	"os"
	"slices"
	"strconv"
	"sync"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapper"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	"github.com/numaproj/numaflow/pkg/shared/callback"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	"github.com/numaproj/numaflow/pkg/udf/forward"
	"github.com/numaproj/numaflow/pkg/udf/rpc"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type MapUDFProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *MapUDFProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	finalWg := sync.WaitGroup{}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fromBuffer := u.VertexInstance.Vertex.OwnedBuffers()
	log = log.With("protocol", "uds-grpc-map-udf")

	var (
		readers            []isb.BufferReader
		writers            map[string][]isb.BufferWriter
		fromVertexWmStores map[string]store.WatermarkStore
		toVertexWmStores   map[string]store.WatermarkStore
		mapHandler         *rpc.GRPCBasedMap
		mapStreamHandler   *rpc.GRPCBasedMap
		idleManager        wmb.IdleManager
		vertexName         = u.VertexInstance.Vertex.Spec.Name
		pipelineName       = u.VertexInstance.Vertex.Spec.PipelineName
	)

	// watermark variables
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList(u.VertexInstance.Vertex.GetToBuffers())
	idleManager = wmb.NewNoOpIdleManager()

	var err error

	// create readers and writers
	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		readers, writers, err = buildRedisBufferIO(ctx, u.VertexInstance)
		if err != nil {
			return err
		}
	case dfv1.ISBSvcTypeJetStream:

		natsClientPool, err := jsclient.NewClientPool(ctx)
		if err != nil {
			return fmt.Errorf("failed to create a new NATS client pool: %w", err)
		}
		defer natsClientPool.CloseAll()

		// multiple go routines can share the same set of writers since nats conn is thread safe
		// https://github.com/nats-io/nats.go/issues/241
		readers, writers, err = buildJetStreamBufferIO(ctx, u.VertexInstance, natsClientPool)
		if err != nil {
			return err
		}

		// created watermark related components only if watermark is enabled
		// otherwise no op will be used
		if !u.VertexInstance.Vertex.Spec.Watermark.Disabled {
			// create from vertex watermark stores
			fromVertexWmStores, err = jetstream.BuildFromVertexWatermarkStores(ctx, u.VertexInstance, natsClientPool.NextAvailableClient())
			if err != nil {
				return fmt.Errorf("failed to build watermark stores: %w", err)
			}

			// create watermark fetcher using watermark stores
			fetchWatermark = fetch.NewEdgeFetcherSet(ctx, u.VertexInstance, fromVertexWmStores, fetch.WithVertexReplica(u.VertexInstance.Replica),
				fetch.WithIsReduce(u.VertexInstance.Vertex.IsReduceUDF()), fetch.WithIsSource(u.VertexInstance.Vertex.IsASource()))

			// create to vertex watermark stores
			toVertexWmStores, err = jetstream.BuildToVertexWatermarkStores(ctx, u.VertexInstance, natsClientPool.NextAvailableClient())
			if err != nil {
				return err
			}

			// create watermark publisher using watermark stores
			publishWatermark = jetstream.BuildPublishersFromStores(ctx, u.VertexInstance, toVertexWmStores)

			idleManager, _ = wmb.NewIdleManager(len(writers), len(writers))
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", u.ISBSvcType)
	}

	opts := []forward.Option{forward.WithLogger(log)}
	enableMapUdfStream := false
	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, sdkclient.DefaultGRPCMaxMessageSize)

	// Wait for map server info to be ready, we use the same info file for all the map modes
	serverInfo, err := serverinfo.SDKServerInfo(serverinfo.WithServerInfoFilePath(sdkclient.MapServerInfoFile))
	if err != nil {
		return err
	}
	metrics.SDKInfo.WithLabelValues(dfv1.ComponentVertex, fmt.Sprintf("%s-%s", pipelineName, vertexName), string(serverinfo.ContainerTypeMapper), serverInfo.Version, string(serverInfo.Language)).Set(1)

	// track all the resources that need to be closed
	var resourcesToClose []io.Closer
	mapMode, ok := serverInfo.Metadata[serverinfo.MapModeKey]

	for index, bufferPartition := range fromBuffer {
		// Read the server info file to read which map mode is enabled
		// Based on the value set, we will create the corresponding handler and clients
		// we create a new client and handler for each partition because
		// the client is not thread safe since we use one common gRPC Bidirectional stream
		// to communicate with the server

		if ok && (serverinfo.MapMode(mapMode) == serverinfo.StreamMap) {
			log.Info("Map mode enabled: Stream Map")
			// Map Stream mode
			enableMapUdfStream = true

			mapStreamClient, err := mapper.New(ctx, serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize), sdkclient.WithUdsSockAddr(sdkclient.MapStreamAddr))
			if err != nil {
				return fmt.Errorf("failed to create map stream client, %w", err)
			}
			mapStreamHandler = rpc.NewUDSgRPCBasedMap(ctx, mapStreamClient, vertexName)

			// Readiness check
			if err := mapStreamHandler.WaitUntilReady(ctx); err != nil {
				return fmt.Errorf("failed on map stream UDF readiness check, %w", err)
			}

			resourcesToClose = append(resourcesToClose, mapStreamHandler)
			opts = append(opts, forward.WithUDFStreamingMap(mapStreamHandler))

		} else if ok && (serverinfo.MapMode(mapMode) == serverinfo.BatchMap) {
			log.Info("Map mode enabled: Batch Map")

			// create the map client with batch map socket address
			mapClient, err := mapper.New(ctx, serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize), sdkclient.WithUdsSockAddr(sdkclient.BatchMapAddr), sdkclient.WithBatchMapMode())
			if err != nil {
				return fmt.Errorf("failed to create batch map client, %w", err)
			}
			mapHandler = rpc.NewUDSgRPCBasedMap(ctx, mapClient, vertexName)
			// Readiness check
			if err := mapHandler.WaitUntilReady(ctx); err != nil {
				return fmt.Errorf("failed on batch map UDF readiness check, %w", err)
			}

			resourcesToClose = append(resourcesToClose, mapHandler)
			opts = append(opts, forward.WithUDFMap(mapHandler))

		} else {
			log.Info("Map mode enabled: Unary Map")
			// Default is to enable unary map mode
			// If the MapMode metadata is not available, we will start map by default this will ensure
			// backward compatibility in case of version mismatch for map

			// create the client and handler for map interface
			mapClient, err := mapper.New(ctx, serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize))
			if err != nil {
				return fmt.Errorf("failed to create map client, %w", err)
			}
			mapHandler = rpc.NewUDSgRPCBasedMap(ctx, mapClient, vertexName)

			// Readiness check
			if err := mapHandler.WaitUntilReady(ctx); err != nil {
				return fmt.Errorf("failed on map UDF readiness check, %w", err)
			}

			resourcesToClose = append(resourcesToClose, mapHandler)
			opts = append(opts, forward.WithUDFMap(mapHandler))
		}

		// Populate shuffle function map
		shuffleFuncMap := make(map[string]*shuffle.Shuffle)
		for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
			if edge.GetToVertexPartitionCount() > 1 {
				s := shuffle.NewShuffle(edge.To, edge.GetToVertexPartitionCount())
				shuffleFuncMap[edge.From+":"+edge.To] = s
			}
		}

		// create a conditional forwarder for each partition
		conditionalForwarder := forwarder.GoWhere(func(keys []string, tags []string, msgId string) ([]forwarder.VertexBuffer, error) {
			var result []forwarder.VertexBuffer

			// Drop message if it contains the special tag
			if slices.Contains(tags, dfv1.MessageTagDrop) {
				metrics.UserDroppedMessages.With(map[string]string{
					metrics.LabelVertex:             vertexName,
					metrics.LabelPipeline:           pipelineName,
					metrics.LabelVertexType:         string(dfv1.VertexTypeMapUDF),
					metrics.LabelVertexReplicaIndex: strconv.Itoa(int(u.VertexInstance.Replica)),
				}).Inc()

				return result, nil
			}

			// Iterate through the edges
			for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
				// Condition to proceed for forwarding message: No conditions on edge, or message tags match edge conditions
				proceed := edge.Conditions == nil || edge.Conditions.Tags == nil || len(edge.Conditions.Tags.Values) == 0 || sharedutil.CompareSlice(edge.Conditions.Tags.GetOperator(), tags, edge.Conditions.Tags.Values)

				if proceed {
					// if the edge has more than one partition, shuffle the message
					// else forward the message to the default partition
					partitionIdx := isb.DefaultPartitionIdx
					if edge.GetToVertexPartitionCount() > 1 {
						edgeKey := edge.From + ":" + edge.To
						if edge.ToVertexType == dfv1.VertexTypeReduceUDF { // Shuffle on keys
							partitionIdx = shuffleFuncMap[edgeKey].ShuffleOnKeys(keys)
						} else { // Shuffle on msgId
							partitionIdx = shuffleFuncMap[edgeKey].ShuffleOnId(msgId)
						}
					}

					result = append(result, forwarder.VertexBuffer{
						ToVertexName:         edge.To,
						ToVertexPartitionIdx: partitionIdx,
					})
				}
			}

			return result, nil
		})
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil {
			if x.ReadBatchSize != nil {
				opts = append(opts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
				opts = append(opts, forward.WithUDFConcurrency(int(*x.ReadBatchSize)))
			}
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
			opts = append(opts, forward.WithCallbackUploader(cbPublisher))
		}

		// create a forwarder for each partition
		df, err := forward.NewInterStepDataForward(u.VertexInstance, readers[index], writers, conditionalForwarder, fetchWatermark, publishWatermark, idleManager, opts...)
		if err != nil {
			return err
		}
		finalWg.Add(1)

		// start the df for each partition using a go routine
		go func(fromBufferPartitionName string, isdf *forward.InterStepDataForward) {
			defer finalWg.Done()
			log.Infow("Start processing udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBufferPartitionName), zap.Any("to", u.VertexInstance.Vertex.GetToBuffers()))
			stopped := isdf.Start()
			select {
			case <-ctx.Done():
				log.Info("Context cancelled, stopping forwarder for partition...", zap.String("partition", fromBufferPartitionName))
				isdf.Stop()
				if err := <-stopped; err != nil {
					log.Errorw("Map forwarder stopped with error", zap.String("fromPartition", fromBufferPartitionName), zap.Error(err))
				}
				log.Info("Exited for partition...", zap.String("partition", fromBufferPartitionName))
			case err := <-stopped:
				if err != nil {
					log.Errorw("Map forwarder stopped with error", zap.String("fromPartition", fromBufferPartitionName), zap.Error(err))
					cancel()
				}
			}
		}(bufferPartition, df)
	}
	// create lag readers from buffer readers
	var lagReaders []isb.LagReader
	for _, reader := range readers {
		lagReaders = append(lagReaders, reader)
	}

	var metricsOpts []metrics.Option
	// Add the correct client handler for the metrics server, based on the mode being used.
	if enableMapUdfStream {
		metricsOpts = metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, []metrics.HealthChecker{mapStreamHandler}, lagReaders)
	} else {
		metricsOpts = metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, []metrics.HealthChecker{mapHandler}, lagReaders)
	}
	ms := metrics.NewMetricsServer(u.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}

	// wait for all the forwarders to exit
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

	// close the fromVertex wm stores
	// since we created the stores, we can close them
	for _, wmStore := range fromVertexWmStores {
		_ = wmStore.Close()
	}

	// close the toVertex wm stores
	// since we created the stores, we can close them
	for _, wmStore := range toVertexWmStores {
		_ = wmStore.Close()
	}

	// close the handlers
	for _, r := range resourcesToClose {
		if err := r.Close(); err != nil {
			log.Errorw("Failed to close the resource", zap.Error(err))
		}
	}

	log.Info("All udf data processors exited...")
	return nil
}
