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
	"time"

	"github.com/numaproj/numaflow/pkg/reduce/pbq/store/wal"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/reduce/pbq"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
	"github.com/numaproj/numaflow/pkg/window/strategy/sliding"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/reduce"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	"github.com/numaproj/numaflow/pkg/udf/function"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/window"
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
	var fromBuffer dfv1.Buffer
	fromBuffers := u.VertexInstance.Vertex.GetFromBuffers()
	// choose the buffer that corresponds to this reduce processor because
	// reducer's incoming edge can have more than one buffer for parallelism
	for _, b := range fromBuffers {
		if strings.HasSuffix(b.Name, fmt.Sprintf("-%d", u.VertexInstance.Replica)) {
			fromBuffer = b
			break
		}
	}
	if len(fromBuffer.Name) == 0 {
		return fmt.Errorf("can not find from buffer")
	}
	// watermark variables
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(u.VertexInstance.Vertex.GetToBuffers()))
	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		reader, writers = buildRedisBufferIO(ctx, fromBuffer.Name, u.VertexInstance)
	case dfv1.ISBSvcTypeJetStream:
		// build watermark progressors
		fetchWatermark, publishWatermark, err = jetstream.BuildWatermarkProgressors(ctx, u.VertexInstance, fromBuffer)
		if err != nil {
			return err
		}
		reader, writers, err = buildJetStreamBufferIO(ctx, fromBuffer.Name, u.VertexInstance)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", u.ISBSvcType)
	}

	// Populate shuffle function map
	shuffleFuncMap := make(map[string]*shuffle.Shuffle)
	for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
		if edge.Parallelism != nil && *edge.Parallelism > 1 {
			s := shuffle.NewShuffle(dfv1.GenerateEdgeBufferNames(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, edge))
			shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)] = s
		}
	}

	conditionalForwarder := forward.GoWhere(func(key string) ([]string, error) {
		result := []string{}
		if key == dfv1.MessageKeyDrop {
			return result, nil
		}
		for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
			// If returned key is not "DROP", and there's no conditions defined in the edge, treat it as "ALL"?
			if edge.Conditions == nil || len(edge.Conditions.KeyIn) == 0 || sharedutil.StringSliceContains(edge.Conditions.KeyIn, key) {
				if edge.Parallelism != nil && *edge.Parallelism > 1 { // Need to shuffle
					result = append(result, shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)].Shuffle(key))
				} else {
					result = append(result, dfv1.GenerateEdgeBufferNames(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, edge)...)
				}
			}
		}
		return result, nil
	})

	log = log.With("protocol", "uds-grpc-reduce-udf")
	udfHandler, err := function.NewUDSGRPCBasedUDF()
	if err != nil {
		return fmt.Errorf("failed to create gRPC client, %w", err)
	}
	// Readiness check
	if err := udfHandler.WaitUntilReady(ctx); err != nil {
		return fmt.Errorf("failed on UDF readiness check, %w", err)
	}
	defer func() {
		err = udfHandler.CloseConn(ctx)
		if err != nil {
			log.Warnw("Failed to close gRPC client conn", zap.Error(err))
		}
	}()
	log.Infow("Start processing reduce udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBuffer.Name))

	storeProvider := wal.NewWALStores(u.VertexInstance, wal.WithStorePath(dfv1.DefaultStorePath), wal.WithMaxBufferSize(dfv1.DefaultStoreMaxBufferSize), wal.WithSyncDuration(dfv1.DefaultStoreSyncDuration))

	pbqManager, err := pbq.NewManager(ctx, u.VertexInstance.Vertex.Spec.Name, u.VertexInstance.Vertex.Spec.PipelineName, storeProvider)
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
	dataForwarder, err := reduce.NewDataForward(ctx, udfHandler, u.VertexInstance, reader, writers, pbqManager, conditionalForwarder, fetchWatermark, publishWatermark, windower, opts...)
	if err != nil {
		return fmt.Errorf("failed get a new DataForward, %w", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dataForwarder.Start()
		log.Info("Forwarder stopped, exiting reduce udf data processor...")
	}()

	metricsOpts := []metrics.Option{
		metrics.WithLookbackSeconds(int64(u.VertexInstance.Vertex.Spec.Scale.GetLookbackSeconds())),
		metrics.WithHealthCheckExecutor(func() error {
			cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			return udfHandler.WaitUntilReady(cctx)
		}),
	}
	if x, ok := reader.(isb.LagReader); ok {
		metricsOpts = append(metricsOpts, metrics.WithLagReader(x))
	}
	if x, ok := reader.(isb.Ratable); ok {
		metricsOpts = append(metricsOpts, metrics.WithRater(x))
	}
	ms := metrics.NewMetricsServer(u.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}

	<-ctx.Done()
	log.Info("SIGTERM, exiting...")
	wg.Wait()
	log.Info("Exited...")
	return nil
}
