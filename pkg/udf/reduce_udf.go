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
	"sync"

	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/sdkclient/reducer"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/udf/rpc"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/store"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/wal"
	"github.com/numaproj/numaflow/pkg/reduce/pnf"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
	"github.com/numaproj/numaflow/pkg/window/strategy/sliding"
)

type ReduceUDFProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *ReduceUDFProcessor) Start(ctx context.Context) error {
	var (
		readers            []isb.BufferReader
		writers            map[string][]isb.BufferWriter
		fromBuffer         string
		err                error
		natsClientPool     *jsclient.ClientPool
		windower           window.Windower
		fromVertexWmStores map[string]store.WatermarkStore
		toVertexWmStores   map[string]store.WatermarkStore
		idleManager        wmb.IdleManager
	)

	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	natsClientPool, err = jsclient.NewClientPool(ctx)
	if err != nil {
		return fmt.Errorf("failed to create a new NATS client pool: %w", err)
	}
	defer natsClientPool.CloseAll()

	f := u.VertexInstance.Vertex.Spec.UDF.GroupBy.Window.Fixed
	s := u.VertexInstance.Vertex.Spec.UDF.GroupBy.Window.Sliding

	if f != nil {
		windower = fixed.NewFixed(f.Length.Duration)
	} else if s != nil {
		windower = sliding.NewSliding(s.Length.Duration, s.Slide.Duration)
	}

	if windower == nil {
		return fmt.Errorf("invalid window spec")
	}

	fromBuffers := u.VertexInstance.Vertex.OwnedBuffers()
	// choose the buffer that corresponds to this reduce processor because
	// reducer's incoming edge can have more than one partitions
	for _, b := range fromBuffers {
		if strings.HasSuffix(b, fmt.Sprintf("-%d", u.VertexInstance.Replica)) {
			fromBuffer = b
			break
		}
	}
	if len(fromBuffer) == 0 {
		return fmt.Errorf("can not find from buffer")
	}
	// watermark variables
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList(u.VertexInstance.Vertex.GetToBuffers())
	idleManager = wmb.NewNoOpIdleManager()
	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		readers, writers, err = buildRedisBufferIO(ctx, u.VertexInstance)
		if err != nil {
			return err
		}
	case dfv1.ISBSvcTypeJetStream:
		// build watermark progressors
		if u.VertexInstance.Vertex.Spec.Watermark.Disabled {
			// use　default no op fetcher, publisher, idleManager
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

	// Populate shuffle function map
	shuffleFuncMap := make(map[string]*shuffle.Shuffle)
	for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
		if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitionCount() > 1 {
			s := shuffle.NewShuffle(edge.To, edge.GetToVertexPartitionCount())
			shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)] = s
		}
	}
	getVertexPartition := GetPartitionedBufferIdx()
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
						ToVertexPartitionIdx: getVertexPartition(edge.To, edge.GetToVertexPartitionCount()),
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
							ToVertexPartitionIdx: getVertexPartition(edge.To, edge.GetToVertexPartitionCount()),
						})
					}
				}
			}
		}

		return result, nil
	})

	log = log.With("protocol", "uds-grpc-reduce-udf")

	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, sdkclient.DefaultGRPCMaxMessageSize)
	sdkClient, err := reducer.New(reducer.WithMaxMessageSize(maxMessageSize))
	if err != nil {
		return fmt.Errorf("failed to create a new gRPC client: %w", err)
	}

	reduceHandler := rpc.NewUDSgRPCBasedReduce(sdkClient)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client, %w", err)
	}
	// Readiness check
	if err := reduceHandler.WaitUntilReady(ctx); err != nil {
		return fmt.Errorf("failed on FIXED_AGGREGATION readiness check, %w", err)
	}
	defer func() {
		err = reduceHandler.CloseConn(ctx)
		if err != nil {
			log.Warnw("Failed to close gRPC client conn", zap.Error(err))
		}
	}()
	log.Infow("Start processing reduce udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBuffer))

	// create lag readers from buffer readers
	var lagReaders []isb.LagReader
	for _, reader := range readers {
		lagReaders = append(lagReaders, reader)
	}

	// start metrics server
	metricsOpts := metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, []metrics.HealthChecker{reduceHandler}, lagReaders)
	ms := metrics.NewMetricsServer(u.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}

	storeProvider := wal.NewWALStores(u.VertexInstance, wal.WithStorePath(dfv1.DefaultStorePath), wal.WithMaxBufferSize(dfv1.DefaultStoreMaxBufferSize), wal.WithSyncDuration(dfv1.DefaultStoreSyncDuration))

	pbqManager, err := pbq.NewManager(ctx, u.VertexInstance.Vertex.Spec.Name, u.VertexInstance.Vertex.Spec.PipelineName, u.VertexInstance.Replica, storeProvider)
	if err != nil {
		log.Errorw("Failed to create pbq manager", zap.Error(err))
		return fmt.Errorf("failed to create pbq manager, %w", err)
	}
	opts := []reduce.Option{}
	if x := u.VertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			opts = append(opts, reduce.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}

	if allowedLateness := u.VertexInstance.Vertex.Spec.UDF.GroupBy.AllowedLateness; allowedLateness != nil {
		opts = append(opts, reduce.WithAllowedLateness(allowedLateness.Duration))
	}

	op := pnf.NewOrderedProcessor(ctx, u.VertexInstance, reduceHandler, writers, pbqManager, conditionalForwarder, publishWatermark, idleManager)

	// for reduce, we read only from one partition
	dataForwarder, err := reduce.NewDataForward(ctx, u.VertexInstance, readers[0], writers, pbqManager, conditionalForwarder, fetchWatermark, publishWatermark, windower, idleManager, op, opts...)
	if err != nil {
		return fmt.Errorf("failed get a new DataForward, %w", err)
	}

	// read the persisted messages before reading the messages from ISB
	err = dataForwarder.ReplayPersistedMessages(ctx)
	if err != nil {
		return fmt.Errorf("failed to read and process persisted messages, %w", err)
	}

	// start reading the ISB messages.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dataForwarder.Start()
		log.Info("Forwarder stopped, exiting reduce udf data processor...")
	}()

	<-ctx.Done()

	log.Info("SIGTERM, exiting...")
	wg.Wait()

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

	log.Info("Exited...")
	return nil
}
