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
	clientsdk "github.com/numaproj/numaflow/pkg/sdkclient/udf/client"
	"sync"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	"github.com/numaproj/numaflow/pkg/udf/function"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
)

type MapUDFProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *MapUDFProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var reader isb.BufferReader
	var writers map[string][]isb.BufferWriter
	var err error
	fromBufferName := u.VertexInstance.Vertex.OwnedBuffers()[0]

	// watermark variables
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList(u.VertexInstance.Vertex.GetToBuffers())

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
		if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitions() > 1 {
			s := shuffle.NewShuffle(u.VertexInstance.Vertex.GetName(), edge.GetToVertexPartitions())
			shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)] = s
		}
	}

	conditionalForwarder := forward.GoWhere(func(keys []string, tags []string) ([]forward.VertexBuffer, error) {
		var result []forward.VertexBuffer

		if sharedutil.StringSliceContains(tags, dfv1.MessageTagDrop) {
			return result, nil
		}

		for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
			// If returned tags is not "DROP", and there's no conditions defined in the edge, treat it as "ALL"?
			if edge.Conditions == nil || edge.Conditions.Tags == nil || len(edge.Conditions.Tags.Values) == 0 {
				if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitions() > 1 { // Need to shuffle
					toVertexPartition := shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)].Shuffle(keys)
					result = append(result, forward.VertexBuffer{
						ToVertexName:      edge.To,
						ToVertexPartition: toVertexPartition,
					})
				} else {
					// TODO: need to shuffle for partitioned map vertex, for now write only to the first partition
					result = append(result, forward.VertexBuffer{
						ToVertexName:      edge.To,
						ToVertexPartition: 0,
					})
				}
			} else {
				if sharedutil.CompareSlice(edge.Conditions.Tags.GetOperator(), tags, edge.Conditions.Tags.Values) {
					if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitions() > 1 { // Need to shuffle
						toVertexPartition := shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)].Shuffle(keys)
						result = append(result, forward.VertexBuffer{
							ToVertexName:      edge.To,
							ToVertexPartition: toVertexPartition,
						})
					} else {
						// TODO: need to shuffle for partitioned map vertex, for now write only to the first partition
						result = append(result, forward.VertexBuffer{
							ToVertexName:      edge.To,
							ToVertexPartition: 0,
						})
					}
				}
			}
		}
		return result, nil
	})

	log = log.With("protocol", "uds-grpc-map-udf")
	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, dfv1.DefaultGRPCMaxMessageSize)
	c, err := clientsdk.New(clientsdk.WithMaxMessageSize(maxMessageSize))
	if err != nil {
		return fmt.Errorf("failed to create a new gRPC client: %w", err)
	}

	udfHandler, err := function.NewUDSgRPCBasedUDF(c)
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
	log.Infow("Start processing udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBufferName), zap.Any("to", u.VertexInstance.Vertex.GetToBuffers()))

	enableMapUdfStream, err := u.VertexInstance.Vertex.MapUdfStreamEnabled()
	if err != nil {
		return fmt.Errorf("failed to parse UDF map streaming metadata, %w", err)
	}

	opts := []forward.Option{forward.WithVertexType(dfv1.VertexTypeMapUDF), forward.WithLogger(log),
		forward.WithUDFStreaming(enableMapUdfStream)}
	if x := u.VertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			opts = append(opts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
			opts = append(opts, forward.WithUDFConcurrency(int(*x.ReadBatchSize)))
		}
	}
	forwarder, err := forward.NewInterStepDataForward(u.VertexInstance.Vertex, reader, writers, conditionalForwarder, udfHandler, fetchWatermark, publishWatermark, opts...)
	if err != nil {
		return err
	}
	stopped := forwarder.Start()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			<-stopped
			log.Info("Forwarder stopped, exiting udf data processor...")
			return
		}
	}()

	metricsOpts := metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, udfHandler, reader)
	ms := metrics.NewMetricsServer(u.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}

	<-ctx.Done()
	log.Info("SIGTERM, exiting...")
	forwarder.Stop()
	wg.Wait()
	log.Info("Exited...")
	return nil
}
