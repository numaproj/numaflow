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

	"go.uber.org/zap"

	"github.com/numaproj/numaflow-go/pkg/function/client"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce"
	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/wal"
	"github.com/numaproj/numaflow/pkg/reduce/pnf"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	"github.com/numaproj/numaflow/pkg/udf/function"
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
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var windower window.Windower
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
	var reader isb.BufferReader
	var writers map[string]isb.BufferWriter
	var err error
	var fromBuffer string
	fromBuffers := u.VertexInstance.Vertex.OwnedBuffers()
	// choose the buffer that corresponds to this reduce processor because
	// reducer's incoming edge can have more than one buffer for parallelism
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
	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		reader, writers = buildRedisBufferIO(ctx, fromBuffer, u.VertexInstance)
	case dfv1.ISBSvcTypeJetStream:
		// build watermark progressors
		fetchWatermark, publishWatermark, err = jetstream.BuildWatermarkProgressors(ctx, u.VertexInstance)
		if err != nil {
			return err
		}
		reader, writers, err = buildJetStreamBufferIO(ctx, fromBuffer, u.VertexInstance)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", u.ISBSvcType)
	}

	// Populate shuffle function map
	shuffleFuncMap := make(map[string]*shuffle.Shuffle)
	for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
		if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitions() > 1 {
			s := shuffle.NewShuffle(u.VertexInstance.Vertex.GetName(), dfv1.GenerateBufferNames(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, edge.To, edge.GetToVertexPartitions()))
			shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)] = s
		}
	}

	conditionalForwarder := forward.GoWhere(func(keys []string, tags []string) ([]string, error) {
		result := []string{}
		if sharedutil.StringSliceContains(tags, dfv1.MessageTagDrop) {
			return result, nil
		}

		for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
			// If returned tags is not "DROP", and there's no conditions defined in the edge, treat it as "ALL"?
			if edge.Conditions == nil || edge.Conditions.Tags == nil || len(edge.Conditions.Tags.Values) == 0 {
				if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitions() > 1 { // Need to shuffle
					result = append(result, shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)].Shuffle(keys))
				} else {
					// TODO: need to shuffle for partitioned map vertex
					result = append(result, dfv1.GenerateBufferNames(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, edge.To, edge.GetToVertexPartitions())...)
				}
			} else {
				if sharedutil.CompareSlice(edge.Conditions.Tags.GetOperator(), tags, edge.Conditions.Tags.Values) {
					if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitions() > 1 { // Need to shuffle
						result = append(result, shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)].Shuffle(keys))
					} else {
						// TODO: need to shuffle for partitioned map vertex
						result = append(result, dfv1.GenerateBufferNames(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, edge.To, edge.GetToVertexPartitions())...)
					}
				}
			}
		}

		return result, nil
	})

	log = log.With("protocol", "uds-grpc-reduce-udf")

	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, dfv1.DefaultGRPCMaxMessageSize)
	c, err := client.New(client.WithMaxMessageSize(maxMessageSize))
	if err != nil {
		return fmt.Errorf("failed to create a new gRPC client: %w", err)
	}

	udfHandler, err := function.NewUDSgRPCBasedUDF(c)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client, %w", err)
	}
	// Readiness check
	if err := udfHandler.WaitUntilReady(ctx); err != nil {
		return fmt.Errorf("failed on FIXED_AGGREGATION readiness check, %w", err)
	}
	defer func() {
		err = udfHandler.CloseConn(ctx)
		if err != nil {
			log.Warnw("Failed to close gRPC client conn", zap.Error(err))
		}
	}()
	log.Infow("Start processing reduce udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBuffer))

	// start metrics server
	metricsOpts := metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, udfHandler, []isb.BufferReader{reader}, nil)
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
	idleManager := wmb.NewIdleManager(len(writers))

	op := pnf.NewOrderedProcessor(ctx, u.VertexInstance, udfHandler, writers, pbqManager, conditionalForwarder, publishWatermark, idleManager)

	dataForwarder, err := reduce.NewDataForward(ctx, u.VertexInstance, reader, writers, pbqManager, conditionalForwarder, fetchWatermark, publishWatermark, windower, idleManager, op, opts...)
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
	log.Info("Exited...")
	return nil
}
