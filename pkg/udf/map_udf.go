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
	"sync"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapper"
	"github.com/numaproj/numaflow/pkg/sdkclient/mapstreamer"
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

	natsClientPool, err := jsclient.NewClientPool(ctx)
	if err != nil {
		return fmt.Errorf("failed to create a new NATS client pool: %w", err)
	}
	defer natsClientPool.CloseAll()

	fromBuffer := u.VertexInstance.Vertex.OwnedBuffers()
	log = log.With("protocol", "uds-grpc-map-udf")

	var (
		readers            []isb.BufferReader
		writers            map[string][]isb.BufferWriter
		fromVertexWmStores map[string]store.WatermarkStore
		toVertexWmStores   map[string]store.WatermarkStore
		mapHandler         *rpc.GRPCBasedMap
		mapStreamHandler   *rpc.GRPCBasedMapStream
		idleManager        wmb.IdleManager
	)

	// watermark variables
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList(u.VertexInstance.Vertex.GetToBuffers())
	idleManager = wmb.NewNoOpIdleManager()

	// create readers and writers
	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		readers, writers, err = buildRedisBufferIO(ctx, u.VertexInstance)
		if err != nil {
			return err
		}
	case dfv1.ISBSvcTypeJetStream:

		// build watermark progressors
		// multiple go routines can share the same set of writers since nats conn is thread safe
		// https://github.com/nats-io/nats.go/issues/241

		if u.VertexInstance.Vertex.Spec.Watermark.Disabled {
			// use default no op fetcher, publisher, idleManager
		} else {
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

			readers, writers, err = buildJetStreamBufferIO(ctx, u.VertexInstance, natsClientPool)
			if err != nil {
				return err
			}
			idleManager = wmb.NewIdleManager(len(writers))
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", u.ISBSvcType)
	}

	enableMapUdfStream, err := u.VertexInstance.Vertex.MapUdfStreamEnabled()
	if err != nil {
		return fmt.Errorf("failed to parse UDF map streaming metadata, %w", err)
	}

	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, sdkclient.DefaultGRPCMaxMessageSize)
	if enableMapUdfStream {
		mapStreamClient, err := mapstreamer.New(mapstreamer.WithMaxMessageSize(maxMessageSize))
		if err != nil {
			return fmt.Errorf("failed to create map stream client, %w", err)
		}
		mapStreamHandler = rpc.NewUDSgRPCBasedMapStream(mapStreamClient)

		// Readiness check
		if err := mapStreamHandler.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on map stream UDF readiness check, %w", err)
		}
		defer func() {
			err = mapStreamHandler.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close gRPC client conn", zap.Error(err))
			}
		}()

	} else {
		mapClient, err := mapper.New(mapper.WithMaxMessageSize(maxMessageSize))
		if err != nil {
			return fmt.Errorf("failed to create map client, %w", err)
		}
		mapHandler = rpc.NewUDSgRPCBasedMap(mapClient)

		// Readiness check
		if err := mapHandler.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on map UDF readiness check, %w", err)
		}
		defer func() {
			err = mapHandler.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close gRPC client conn", zap.Error(err))
			}
		}()
	}

	for index, bufferPartition := range fromBuffer {
		// Populate shuffle function map
		shuffleFuncMap := make(map[string]*shuffle.Shuffle)
		for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
			if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitionCount() > 1 {
				s := shuffle.NewShuffle(edge.To, edge.GetToVertexPartitionCount())
				shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)] = s
			}
		}

		// create a conditional forwarder for each partition
		getVertexPartitionIdx := GetPartitionedBufferIdx()
		conditionalForwarder := forwarder.GoWhere(func(keys []string, tags []string) ([]forwarder.VertexBuffer, error) {
			var result []forwarder.VertexBuffer

			if sharedutil.StringSliceContains(tags, dfv1.MessageTagDrop) {
				return result, nil
			}

			for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
				// If returned tags is not "DROP", and there's no conditions defined in the edge, treat it as "ALL"?
				if edge.Conditions == nil || edge.Conditions.Tags == nil || len(edge.Conditions.Tags.Values) == 0 {
					if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitionCount() > 1 { // Need to shuffle
						toVertexPartition := shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)].Shuffle(keys)
						result = append(result, forwarder.VertexBuffer{
							ToVertexName:         edge.To,
							ToVertexPartitionIdx: toVertexPartition,
						})
					} else {
						result = append(result, forwarder.VertexBuffer{
							ToVertexName:         edge.To,
							ToVertexPartitionIdx: getVertexPartitionIdx(edge.To, edge.GetToVertexPartitionCount()),
						})
					}
				} else {
					if sharedutil.CompareSlice(edge.Conditions.Tags.GetOperator(), tags, edge.Conditions.Tags.Values) {
						if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitionCount() > 1 { // Need to shuffle
							toVertexPartition := shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)].Shuffle(keys)
							result = append(result, forwarder.VertexBuffer{
								ToVertexName:         edge.To,
								ToVertexPartitionIdx: toVertexPartition,
							})
						} else {
							result = append(result, forwarder.VertexBuffer{
								ToVertexName:         edge.To,
								ToVertexPartitionIdx: getVertexPartitionIdx(edge.To, edge.GetToVertexPartitionCount()),
							})
						}
					}
				}
			}
			return result, nil
		})

		opts := []forward.Option{forward.WithLogger(log),
			forward.WithUDFStreaming(enableMapUdfStream)}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil {
			if x.ReadBatchSize != nil {
				opts = append(opts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
				opts = append(opts, forward.WithUDFConcurrency(int(*x.ReadBatchSize)))
			}
		}
		// create a forwarder for each partition
		forwarder, err := forward.NewInterStepDataForward(u.VertexInstance, readers[index], writers, conditionalForwarder, mapHandler, mapStreamHandler, fetchWatermark, publishWatermark, idleManager, opts...)
		if err != nil {
			return err
		}
		finalWg.Add(1)

		// start the forwarder for each partition using a go routine
		go func(fromBufferPartitionName string, isdf *forward.InterStepDataForward) {
			defer finalWg.Done()
			log.Infow("Start processing udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBufferPartitionName), zap.Any("to", u.VertexInstance.Vertex.GetToBuffers()))

			stopped := forwarder.Start()
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					<-stopped
					log.Info("Forwarder stopped, exiting udf data processor for partition " + fromBufferPartitionName + "...")
					return
				}
			}()

			<-ctx.Done()
			log.Info("SIGTERM, exiting inside partition...", zap.String("partition", fromBufferPartitionName))
			forwarder.Stop()
			wg.Wait()
			log.Info("Exited for partition...", zap.String("partition", fromBufferPartitionName))
		}(bufferPartition, forwarder)
	}
	// create lag readers from buffer readers
	var lagReaders []isb.LagReader
	for _, reader := range readers {
		lagReaders = append(lagReaders, reader)
	}

	var metricsOpts []metrics.Option
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

	// close the from vertex wm stores
	// since we created the stores, we can close them
	for _, wmStore := range fromVertexWmStores {
		_ = wmStore.Close()
	}

	// close the to vertex wm stores
	// since we created the stores, we can close them
	for _, wmStore := range toVertexWmStores {
		_ = wmStore.Close()
	}

	log.Info("All udf data processors exited...")
	return nil
}

// GetPartitionedBufferIdx returns a function that returns a partitioned buffer index based on the toVertex name and the partition count
// it distributes the messages evenly to the partitions of the toVertex based on the message count(round-robin)
func GetPartitionedBufferIdx() func(toVertex string, toVertexPartitionCount int) int32 {
	messagePerPartitionMap := make(map[string]int)
	return func(toVertex string, toVertexPartitionCount int) int32 {
		toVertexPartition := (messagePerPartitionMap[toVertex] + 1) % toVertexPartitionCount
		messagePerPartitionMap[toVertex] = toVertexPartition
		return int32(toVertexPartition)
	}
}
