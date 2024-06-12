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
	"sync"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/flatmap/forward"
	"github.com/numaproj/numaflow/pkg/flatmap/rpc"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/sdkclient/flatmapper"
	sdkserverinfo "github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type FlatmapUDFProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *FlatmapUDFProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	finalWg := sync.WaitGroup{}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fromBuffer := u.VertexInstance.Vertex.OwnedBuffers()
	log = log.With("protocol", "uds-grpc-flatmap-udf")

	var (
		readers            []isb.BufferReader
		writers            map[string][]isb.BufferWriter
		fromVertexWmStores map[string]store.WatermarkStore
		toVertexWmStores   map[string]store.WatermarkStore
		flatmapHandler     *rpc.GRPCBasedFlatmap
		idleManager        wmb.IdleManager
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
		// otherwise no op will used
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

	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, sdkclient.DefaultGRPCMaxMessageSize)
	// Wait for server info to be ready
	serverInfo, err := sdkserverinfo.SDKServerInfo(sdkserverinfo.WithServerInfoFilePath(sdkclient.FlatmapServerInfoFile))
	if err != nil {
		return err
	}

	flatmapClient, err := flatmapper.New(serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize))
	if err != nil {
		return fmt.Errorf("failed to create map client, %w", err)
	}
	flatmapHandler = rpc.NewUDSgRPCBasedFlatmap(flatmapClient, u.VertexInstance.Vertex.Spec.Name)

	// Readiness check
	if err := flatmapHandler.WaitUntilReady(ctx); err != nil {
		return fmt.Errorf("failed on map UDF readiness check, %w", err)
	}
	defer func() {
		err = flatmapHandler.CloseConn(ctx)
		if err != nil {
			log.Warnw("Failed to close gRPC client conn", zap.Error(err))
		}
	}()

	for index, bufferPartition := range fromBuffer {
		// Populate shuffle function map
		shuffleFuncMap := make(map[string]*shuffle.Shuffle)
		for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
			if edge.GetToVertexPartitionCount() > 1 {
				s := shuffle.NewShuffle(edge.To, edge.GetToVertexPartitionCount())
				shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)] = s
			}
		}

		// create a conditional forwarder for each partition
		conditionalForwarder := forwarder.GoWhere(func(keys []string, tags []string, msgId string) ([]forwarder.VertexBuffer, error) {
			var result []forwarder.VertexBuffer

			// Drop message if it contains the special tag
			if sharedutil.StringSliceContains(tags, dfv1.MessageTagDrop) {
				return result, nil
			}

			//var idString strings.Builder
			// Iterate through the edges
			for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
				//idString.Reset()
				//idString.WriteString(edge.From)
				//idString.WriteString(":")
				//idString.WriteString(edge.To)
				//edgeKey := idString.String()
				edgeKey := fmt.Sprintf("%s:%s", edge.From, edge.To)

				// Condition to proceed for forwarding message: No conditions on edge, or message tags match edge conditions
				proceed := edge.Conditions == nil || edge.Conditions.Tags == nil || len(edge.Conditions.Tags.Values) == 0 || sharedutil.CompareSlice(edge.Conditions.Tags.GetOperator(), tags, edge.Conditions.Tags.Values)

				if proceed {
					// if the edge has more than one partition, shuffle the message
					// else forward the message to the default partition
					partitionIdx := isb.DefaultPartitionIdx
					if edge.GetToVertexPartitionCount() > 1 {
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

		opts := []forward.Option{}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil {
			if x.ReadBatchSize != nil {
				opts = append(opts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
				opts = append(opts, forward.WithUDFConcurrency(int(*x.ReadBatchSize)))
			}
		}
		// create a forwarder for each partition
		df, err := forward.NewInterStepDataForward(u.VertexInstance, readers[index], writers, conditionalForwarder, flatmapHandler, fetchWatermark, publishWatermark, idleManager, opts...)
		if err != nil {
			return err
		}
		finalWg.Add(1)

		// start the df for each partition using a go routine
		go func(fromBufferPartitionName string, isdf *forward.InterStepDataForward) {
			defer finalWg.Done()
			log.Infow("Start processing udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBufferPartitionName), zap.Any("to", u.VertexInstance.Vertex.GetToBuffers()))

			stopped := isdf.Start()
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
			isdf.Stop()
			wg.Wait()
			log.Info("Exited for partition...", zap.String("partition", fromBufferPartitionName))
		}(bufferPartition, df)
	}
	// create lag readers from buffer readers
	var lagReaders []isb.LagReader
	for _, reader := range readers {
		lagReaders = append(lagReaders, reader)
	}

	var metricsOpts []metrics.Option
	metricsOpts = metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, []metrics.HealthChecker{flatmapHandler}, lagReaders)

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
