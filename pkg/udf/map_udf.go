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

	clientsdk "github.com/numaproj/numaflow/pkg/sdkclient/udf/client"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"

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

	// create readers and writers
	var readers []isb.BufferReader
	var writers map[string][]isb.BufferWriter
	// watermark variables
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList(u.VertexInstance.Vertex.GetToBuffers())

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
		fetchWatermark, publishWatermark, err = jetstream.BuildWatermarkProgressors(ctx, u.VertexInstance, natsClientPool.NextAvailableClient())
		if err != nil {
			return err
		}
		readers, writers, err = buildJetStreamBufferIO(ctx, u.VertexInstance, natsClientPool)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", u.ISBSvcType)
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
		conditionalForwarder := forward.GoWhere(func(keys []string, tags []string) ([]forward.VertexBuffer, error) {
			var result []forward.VertexBuffer

			if sharedutil.StringSliceContains(tags, dfv1.MessageTagDrop) {
				return result, nil
			}

			for _, edge := range u.VertexInstance.Vertex.Spec.ToEdges {
				// If returned tags is not "DROP", and there's no conditions defined in the edge, treat it as "ALL"?
				if edge.Conditions == nil || edge.Conditions.Tags == nil || len(edge.Conditions.Tags.Values) == 0 {
					if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitionCount() > 1 { // Need to shuffle
						toVertexPartition := shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)].Shuffle(keys)
						result = append(result, forward.VertexBuffer{
							ToVertexName:         edge.To,
							ToVertexPartitionIdx: toVertexPartition,
						})
					} else {
						result = append(result, forward.VertexBuffer{
							ToVertexName:         edge.To,
							ToVertexPartitionIdx: getVertexPartitionIdx(edge.To, edge.GetToVertexPartitionCount()),
						})
					}
				} else {
					if sharedutil.CompareSlice(edge.Conditions.Tags.GetOperator(), tags, edge.Conditions.Tags.Values) {
						if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitionCount() > 1 { // Need to shuffle
							toVertexPartition := shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)].Shuffle(keys)
							result = append(result, forward.VertexBuffer{
								ToVertexName:         edge.To,
								ToVertexPartitionIdx: toVertexPartition,
							})
						} else {
							result = append(result, forward.VertexBuffer{
								ToVertexName:         edge.To,
								ToVertexPartitionIdx: getVertexPartitionIdx(edge.To, edge.GetToVertexPartitionCount()),
							})
						}
					}
				}
			}
			return result, nil
		})

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
		// create a forwarder for each partition
		forwarder, err := forward.NewInterStepDataForward(u.VertexInstance.Vertex, readers[index], writers, conditionalForwarder, udfHandler, fetchWatermark, publishWatermark, opts...)
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
	metricsOpts := metrics.NewMetricsOptions(ctx, u.VertexInstance.Vertex, []metrics.HealthChecker{udfHandler}, readers)
	ms := metrics.NewMetricsServer(u.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}
	// wait for all the forwarders to exit
	finalWg.Wait()

	// Close the watermark fetcher and publisher
	err = fetchWatermark.Close()
	if err != nil {
		log.Error("Failed to close the watermark fetcher", zap.Error(err))
	}

	for _, publisher := range publishWatermark {
		err = publisher.Close()
		if err != nil {
			log.Error("Failed to close the watermark publisher", zap.Error(err))
		}
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
