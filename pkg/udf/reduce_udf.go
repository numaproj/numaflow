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

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/reduce"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	"github.com/numaproj/numaflow/pkg/udf/function"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
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
	if x := u.VertexInstance.Vertex.Spec.UDF.GroupBy.Window.Fixed; x != nil {
		windower = fixed.NewFixed(x.Length.Duration)
	}
	if windower == nil {
		return fmt.Errorf("invalid window spec")
	}
	var reader isb.BufferReader
	var writers map[string]isb.BufferWriter
	var err error
	fromBuffers := u.VertexInstance.Vertex.GetFromBuffers()
	var fromBufferName string
	for _, b := range fromBuffers {
		if strings.HasSuffix(b.Name, fmt.Sprintf("-%d", u.VertexInstance.Replica)) {
			fromBufferName = b.Name
			break
		}
	}
	if len(fromBufferName) == 0 {
		return fmt.Errorf("can not find from buffer")
	}
	// watermark variables
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(u.VertexInstance.Vertex.GetToBuffers()))
	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		reader, writers = buildRedisBufferIO(ctx, fromBufferName, u.VertexInstance)
	case dfv1.ISBSvcTypeJetStream:
		// build watermark progressors
		fetchWatermark, publishWatermark, err = jetstream.BuildWatermarkProgressors(ctx, u.VertexInstance)
		if err != nil {
			return err
		}
		reader, writers, err = buildJetStreamBufferIO(ctx, fromBufferName, u.VertexInstance)
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
	log.Infow("Start processing reduce udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBufferName))

	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithPbqStoreType(dfv1.InMemoryType)))

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
	dataforwarder, err := reduce.NewDataForward(ctx, udfHandler, reader, writers, pbqManager, conditionalForwarder, fetchWatermark, publishWatermark, windower, opts...)
	if err != nil {
		return fmt.Errorf("failed get a new DataForward, %w", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dataforwarder.Start(ctx)
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
