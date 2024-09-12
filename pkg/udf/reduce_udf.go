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
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce"
	"github.com/numaproj/numaflow/pkg/reduce/applier"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	alignedfs "github.com/numaproj/numaflow/pkg/reduce/pbq/wal/aligned/fs"
	noopwal "github.com/numaproj/numaflow/pkg/reduce/pbq/wal/noop"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/wal/unaligned"
	unalignedfs "github.com/numaproj/numaflow/pkg/reduce/pbq/wal/unaligned/fs"
	"github.com/numaproj/numaflow/pkg/reduce/pnf"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/sdkclient/reducer"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	"github.com/numaproj/numaflow/pkg/sdkclient/sessionreducer"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	"github.com/numaproj/numaflow/pkg/udf/rpc"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
	"github.com/numaproj/numaflow/pkg/window/strategy/session"
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
		windower           window.TimedWindower
		fromVertexWmStores map[string]store.WatermarkStore
		toVertexWmStores   map[string]store.WatermarkStore
		idleManager        wmb.IdleManager
		opts               []reduce.Option
		udfApplier         applier.ReduceApplier
		healthChecker      metrics.HealthChecker
		pipelineName       = u.VertexInstance.Vertex.Spec.PipelineName
		vertexName         = u.VertexInstance.Vertex.Name
		vertexReplica      = u.VertexInstance.Replica
	)

	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	windowType := u.VertexInstance.Vertex.Spec.UDF.GroupBy.Window

	// based on the window type create the windower, udfApplier and health checker
	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, sdkclient.DefaultGRPCMaxMessageSize)

	// create udf handler and wait until it is ready
	if windowType.Fixed != nil || windowType.Sliding != nil {
		var serverInfo *serverinfo.ServerInfo
		var client reducer.Client
		// if streaming is enabled, use the reduceStreaming address
		if (windowType.Fixed != nil && windowType.Fixed.Streaming) || (windowType.Sliding != nil && windowType.Sliding.Streaming) {
			// Wait for server info to be ready
			serverInfo, err = serverinfo.SDKServerInfo(serverinfo.WithServerInfoFilePath(sdkclient.ReduceStreamServerInfoFile))
			if err != nil {
				return err
			}
			client, err = reducer.New(serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize), sdkclient.WithUdsSockAddr(sdkclient.ReduceStreamAddr))
		} else {
			// Wait for server info to be ready
			serverInfo, err = serverinfo.SDKServerInfo(serverinfo.WithServerInfoFilePath(sdkclient.ReduceServerInfoFile))
			if err != nil {
				return err
			}
			client, err = reducer.New(serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize))
		}
		if err != nil {
			return fmt.Errorf("failed to create a new reducer gRPC client: %w", err)
		}

		reduceHandler := rpc.NewUDSgRPCAlignedReduce(u.VertexInstance.Vertex.Name, u.VertexInstance.Replica, client)
		// Readiness check
		if err := reduceHandler.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on udf readiness check, %w", err)
		}
		defer func() {
			err = reduceHandler.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close gRPC client conn", zap.Error(err))
			}
		}()

		udfApplier = reduceHandler
		healthChecker = reduceHandler
	} else if windowType.Session != nil {
		// Wait for server info to be ready
		serverInfo, err := serverinfo.SDKServerInfo(serverinfo.WithServerInfoFilePath(sdkclient.SessionReduceServerInfoFile))
		if err != nil {
			return err
		}

		client, err := sessionreducer.New(serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize))
		if err != nil {
			return fmt.Errorf("failed to create a new session reducer gRPC client: %w", err)
		}

		reduceHandler := rpc.NewGRPCBasedUnalignedReduce(vertexName, client)
		// Readiness check
		if err := reduceHandler.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on udf readiness check, %w", err)
		}
		defer func() {
			err = reduceHandler.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close gRPC client conn", zap.Error(err))
			}
		}()

		udfApplier = reduceHandler
		healthChecker = reduceHandler
	} else {
		return fmt.Errorf("invalid window spec")
	}

	// create windower
	if windowType.Fixed != nil {
		windower = fixed.NewWindower(windowType.Fixed.Length.Duration, u.VertexInstance)
	} else if windowType.Sliding != nil {
		windower = sliding.NewWindower(windowType.Sliding.Length.Duration, windowType.Sliding.Slide.Duration, u.VertexInstance)
	} else if windowType.Session != nil {
		windower = session.NewWindower(windowType.Session.Timeout.Duration, u.VertexInstance)
	} else {
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

		natsClientPool, err = jsclient.NewClientPool(ctx)
		if err != nil {
			return fmt.Errorf("failed to create a new NATS client pool: %w", err)
		}
		defer natsClientPool.CloseAll()

		readers, writers, err = buildJetStreamBufferIO(ctx, u.VertexInstance, natsClientPool)
		if err != nil {
			return err
		}

		// created watermark related components only if watermark is enabled
		// otherwise noop will used
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

			idleManager, _ = wmb.NewIdleManager(1, len(writers))
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", u.ISBSvcType)
	}

	// Populate shuffle function map
	shuffleFuncMap := make(map[string]*shuffle.Shuffle)
	for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
		if edge.GetToVertexPartitionCount() > 1 {
			s := shuffle.NewShuffle(edge.To, edge.GetToVertexPartitionCount())
			shuffleFuncMap[edge.From+":"+edge.To] = s
		}
	}

	// create the conditional forwarder
	conditionalForwarder := forwarder.GoWhere(func(keys []string, tags []string, msgId string) ([]forwarder.VertexBuffer, error) {
		var result []forwarder.VertexBuffer

		// Drop message if it contains the special tag
		if sharedutil.StringSliceContains(tags, dfv1.MessageTagDrop) {
			metrics.UserDroppedMessages.With(map[string]string{
				metrics.LabelVertex:             vertexName,
				metrics.LabelPipeline:           pipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeReduceUDF),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(u.VertexInstance.Replica)),
			}).Inc()
			return result, nil
		}

		// Iterate through the edges
		for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
			edgeKey := edge.From + ":" + edge.To

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

	log.Infow("Start processing reduce udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBuffer))

	// create lag readers from buffer readers
	var lagReaders []isb.LagReader
	for _, reader := range readers {
		lagReaders = append(lagReaders, reader)
	}

	// start metrics server
	metricsOpts := metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, []metrics.HealthChecker{healthChecker}, lagReaders)
	ms := metrics.NewMetricsServer(u.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}

	// create noop wal manager
	walManager := noopwal.NewNoopStores()
	// if the vertex has a persistent volume claim or empty dir, create a file system based wal manager
	if u.VertexInstance.Vertex.Spec.UDF.GroupBy.Storage.PersistentVolumeClaim != nil ||
		u.VertexInstance.Vertex.Spec.UDF.GroupBy.Storage.EmptyDir != nil {
		if windower.Type() == window.Aligned {
			walManager = alignedfs.NewFSManager(u.VertexInstance)
		} else {
			walManager = unalignedfs.NewFSManager(ctx, dfv1.DefaultSegmentWALPath, dfv1.DefaultCompactWALPath, u.VertexInstance)
		}
	}

	pbqManager, err := pbq.NewManager(ctx, vertexName, pipelineName, vertexReplica, walManager, windower.Type())
	if err != nil {
		log.Errorw("Failed to create pbq manager", zap.Error(err))
		return fmt.Errorf("failed to create pbq manager, %w", err)
	}

	if x := u.VertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			opts = append(opts, reduce.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}

	if allowedLateness := u.VertexInstance.Vertex.Spec.UDF.GroupBy.AllowedLateness; allowedLateness != nil {
		opts = append(opts, reduce.WithAllowedLateness(allowedLateness.Duration))
	}

	var pnfOption []pnf.Option
	// create and start the compactor if the window type is unaligned
	// the compactor will delete the persisted messages which belongs to the materialized window
	// create a gc events tracker which tracks the gc events, will be used by the pnf
	// to track the gc events and the compactor will delete the persisted messages based on the gc events
	if windowType.Session != nil {
		gcEventsTracker, err := unalignedfs.NewGCEventsWAL(ctx, pipelineName, vertexName, vertexReplica)
		if err != nil {
			return fmt.Errorf("failed to create gc events tracker, %w", err)
		}

		// close the gc events tracker
		defer func() {
			err = gcEventsTracker.Close()
			if err != nil {
				log.Errorw("failed to close gc events tracker", zap.Error(err))
			}
			log.Info("GC Events WAL Closed")
		}()

		pnfOption = append(pnfOption, pnf.WithGCEventsTracker(gcEventsTracker), pnf.WithWindowType(window.Unaligned))

		compactor, err := unalignedfs.NewCompactor(ctx, pipelineName, vertexName, vertexReplica, &window.SharedUnalignedPartition, dfv1.DefaultGCEventsWALEventsPath, dfv1.DefaultSegmentWALPath, dfv1.DefaultCompactWALPath)
		if err != nil {
			return fmt.Errorf("failed to create compactor, %w", err)
		}
		err = compactor.Start(ctx)
		if err != nil {
			return fmt.Errorf("failed to start compactor, %w", err)
		}
		defer func(compactor unaligned.Compactor) {
			err = compactor.Stop()
			if err != nil {
				log.Errorw("failed to stop compactor", zap.Error(err))
			}
			log.Info("Compactor Stopped")
		}(compactor)
	}

	// create the pnf
	processAndForward := pnf.NewProcessAndForward(ctx, u.VertexInstance, udfApplier, writers, pbqManager, conditionalForwarder, publishWatermark, idleManager, windower, pnfOption...)

	// for reduce, we read only from one partition
	dataForwarder, err := reduce.NewDataForward(ctx, u.VertexInstance, readers[0], writers, pbqManager, walManager, conditionalForwarder, fetchWatermark, publishWatermark, windower, idleManager, processAndForward, opts...)
	if err != nil {
		return fmt.Errorf("failed get a new DataForward, %w", err)
	}

	// read the persisted messages before reading the messages from ISB
	err = dataForwarder.ReplayPersistedMessages(ctx)
	if err != nil {
		log.Errorw("Failed to read and process persisted messages", zap.Error(err))
		return fmt.Errorf("failed to read and process persisted messages, %w", err)
	}

	// start reading the ISB messages.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dataForwarder.Start()
		log.Info("Forwarder stopped, exiting reduce udf data processor...")

		// after exiting from pbq write loop, we need to gracefully shut down the pnf
		processAndForward.Shutdown()
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
