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

package sources

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forward"
	"github.com/numaproj/numaflow/pkg/forward/applier"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/stores/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/metrics"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	"github.com/numaproj/numaflow/pkg/sources/generator"
	"github.com/numaproj/numaflow/pkg/sources/http"
	"github.com/numaproj/numaflow/pkg/sources/kafka"
	"github.com/numaproj/numaflow/pkg/sources/nats"
	"github.com/numaproj/numaflow/pkg/sources/redisstreams"
	"github.com/numaproj/numaflow/pkg/sources/transformer"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/store/noop"
)

type SourceProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (sp *SourceProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var writersMap = make(map[string][]isb.BufferWriter)

	// watermark variables no-op initialization
	// publishWatermark is a map representing a progressor per edge, we are initializing them to a no-op progressor
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromBufferList(sp.VertexInstance.Vertex.GetToBuffers())
	var sourcePublisherStores = store.BuildWatermarkStore(noop.NewKVNoOpStore(), noop.NewKVNoOpStore())
	var err error

	switch sp.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		for _, e := range sp.VertexInstance.Vertex.Spec.ToEdges {
			writeOpts := []redisclient.Option{
				redisclient.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
			}
			if x := e.ToVertexLimits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, redisclient.WithMaxLength(int64(*x.BufferMaxLength)))
			} else if x := e.DeprecatedLimits; x != nil && x.BufferMaxLength != nil {
				// TODO: remove this branch when deprecated limits are removed
				writeOpts = append(writeOpts, redisclient.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.ToVertexLimits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, redisclient.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			} else if x := e.DeprecatedLimits; x != nil && x.BufferUsageLimit != nil {
				// TODO: remove this branch when deprecated limits are removed
				writeOpts = append(writeOpts, redisclient.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			buffers := dfv1.GenerateBufferNames(sp.VertexInstance.Vertex.Namespace, sp.VertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitions())
			var bufferWriters []isb.BufferWriter
			for _, partition := range buffers {
				group := partition + "-group"
				redisClient := redisclient.NewInClusterRedisClient()
				writer := redisisb.NewBufferWrite(ctx, redisClient, partition, group, writeOpts...)
				bufferWriters = append(bufferWriters, writer)
			}
			writersMap[e.To] = bufferWriters
		}
	case dfv1.ISBSvcTypeJetStream:
		// build watermark progressors
		fetchWatermark, publishWatermark, err = jetstream.BuildWatermarkProgressors(ctx, sp.VertexInstance)
		if err != nil {
			return err
		}

		sourcePublisherStores, err = jetstream.BuildSourcePublisherStores(ctx, sp.VertexInstance)
		if err != nil {
			return err
		}
		for _, e := range sp.VertexInstance.Vertex.Spec.ToEdges {
			writeOpts := []jetstreamisb.WriteOption{
				jetstreamisb.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
			}
			if x := e.ToVertexLimits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
			} else if x := e.DeprecatedLimits; x != nil && x.BufferMaxLength != nil {
				// TODO: remove this branch when deprecated limits are removed
				writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.ToVertexLimits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			} else if x := e.DeprecatedLimits; x != nil && x.BufferUsageLimit != nil {
				// TODO: remove this branch when deprecated limits are removed
				writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			var bufferWriters []isb.BufferWriter
			buffers := dfv1.GenerateBufferNames(sp.VertexInstance.Vertex.Namespace, sp.VertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitions())
			for _, buffer := range buffers {
				streamName := isbsvc.JetStreamName(buffer)
				jetStreamClient := jsclient.NewInClusterJetStreamClient()
				writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jetStreamClient, buffer, streamName, streamName, writeOpts...)
				if err != nil {
					return err
				}
				bufferWriters = append(bufferWriters, writer)
			}

			writersMap[e.To] = bufferWriters
		}
	default:
		return fmt.Errorf("unrecognized isb svc type %q", sp.ISBSvcType)
	}
	var sourcer Sourcer
	var readyChecker metrics.HealthChecker
	// Populate shuffle function map
	// we need to shuffle the messages, because we can have a reduce vertex immediately after a source vertex.
	var toVertexPartitionMap = make(map[string]int)
	shuffleFuncMap := make(map[string]*shuffle.Shuffle)
	for _, edge := range sp.VertexInstance.Vertex.Spec.ToEdges {
		if edge.ToVertexType == dfv1.VertexTypeReduceUDF && edge.GetToVertexPartitions() > 1 {
			s := shuffle.NewShuffle(sp.VertexInstance.Vertex.GetName(), edge.GetToVertexPartitions())
			shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)] = s
		}
		toVertexPartitionMap[edge.To] = edge.GetToVertexPartitions()
	}
	if sp.VertexInstance.Vertex.HasUDTransformer() {
		t, err := transformer.NewGRPCBasedTransformer()
		if err != nil {
			return fmt.Errorf("failed to create gRPC client, %w", err)
		}
		// Readiness check
		if err = t.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on user defined transformer readiness check, %w", err)
		}
		defer func() {
			err = t.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close gRPC client conn", zap.Error(err))
			}
		}()
		readyChecker = t
		sourcer, err = sp.getSourcer(writersMap, sp.getTransformerGoWhereDecider(shuffleFuncMap), t, fetchWatermark, publishWatermark, sourcePublisherStores, log)
	} else {
		sourcer, err = sp.getSourcer(writersMap, sp.getSourceGoWhereDecider(shuffleFuncMap), applier.Terminal, fetchWatermark, publishWatermark, sourcePublisherStores, log)
	}
	if err != nil {
		return fmt.Errorf("failed to find a sourcer, error: %w", err)
	}
	log.Infow("Start processing source messages", zap.String("isbs", string(sp.ISBSvcType)), zap.Any("to", sp.VertexInstance.Vertex.GetToBuffers()))
	stopped := sourcer.Start()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			<-stopped
			log.Info("Sourcer stopped, exiting source processor...")
			return
		}
	}()

	metricsOpts := metrics.NewMetricsOptions(ctx, sp.VertexInstance.Vertex, readyChecker, []isb.BufferReader{sourcer})
	ms := metrics.NewMetricsServer(sp.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}

	<-ctx.Done()
	log.Info("SIGTERM, exiting...")
	sourcer.Stop()
	wg.Wait()
	log.Info("Exited...")
	return nil
}

// getSourcer is used to send the sourcer information
func (sp *SourceProcessor) getSourcer(writers map[string][]isb.BufferWriter,
	fsd forward.ToWhichStepDecider,
	mapApplier applier.MapApplier,
	fetchWM fetch.Fetcher,
	publishWM map[string]publish.Publisher,
	publishWMStores store.WatermarkStorer,
	logger *zap.SugaredLogger) (Sourcer, error) {

	src := sp.VertexInstance.Vertex.Spec.Source
	if x := src.Generator; x != nil {
		readOptions := []generator.Option{
			generator.WithLogger(logger),
		}
		if l := sp.VertexInstance.Vertex.Spec.Limits; l != nil && l.ReadTimeout != nil {
			readOptions = append(readOptions, generator.WithReadTimeout(l.ReadTimeout.Duration))
		}
		return generator.NewMemGen(sp.VertexInstance, writers, fsd, mapApplier, fetchWM, publishWM, publishWMStores, readOptions...)
	} else if x := src.Kafka; x != nil {
		readOptions := []kafka.Option{
			kafka.WithGroupName(x.ConsumerGroupName),
			kafka.WithLogger(logger),
		}
		if l := sp.VertexInstance.Vertex.Spec.Limits; l != nil && l.ReadTimeout != nil {
			readOptions = append(readOptions, kafka.WithReadTimeOut(l.ReadTimeout.Duration))
		}
		return kafka.NewKafkaSource(sp.VertexInstance, writers, fsd, mapApplier, fetchWM, publishWM, publishWMStores, readOptions...)
	} else if x := src.HTTP; x != nil {
		return http.New(sp.VertexInstance, writers, fsd, mapApplier, fetchWM, publishWM, publishWMStores, http.WithLogger(logger))
	} else if x := src.Nats; x != nil {
		readOptions := []nats.Option{
			nats.WithLogger(logger),
		}
		if l := sp.VertexInstance.Vertex.Spec.Limits; l != nil && l.ReadTimeout != nil {
			readOptions = append(readOptions, nats.WithReadTimeout(l.ReadTimeout.Duration))
		}
		return nats.New(sp.VertexInstance, writers, fsd, mapApplier, fetchWM, publishWM, publishWMStores, readOptions...)
	} else if x := src.RedisStreams; x != nil {
		readOptions := []redisstreams.Option{
			redisstreams.WithLogger(logger),
		}
		if l := sp.VertexInstance.Vertex.Spec.Limits; l != nil && l.ReadTimeout != nil {
			readOptions = append(readOptions, redisstreams.WithReadTimeOut(l.ReadTimeout.Duration))
		}
		return redisstreams.New(sp.VertexInstance, writers, fsd, mapApplier, fetchWM, publishWM, publishWMStores, readOptions...)
	}
	return nil, fmt.Errorf("invalid source spec")
}

func (sp *SourceProcessor) getSourceGoWhereDecider(shuffleFuncMap map[string]*shuffle.Shuffle) forward.GoWhere {
	getToBufferPartition := GetPartitionedBufferName()

	fsd := forward.GoWhere(func(keys []string, tags []string) ([]forward.VertexBuffer, error) {
		var result []forward.VertexBuffer

		for _, edge := range sp.VertexInstance.Vertex.Spec.ToEdges {
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
					ToVertexPartition: getToBufferPartition(edge.To, edge.GetToVertexPartitions()),
				})
			}
		}
		return result, nil
	})
	return fsd
}

func (sp *SourceProcessor) getTransformerGoWhereDecider(shuffleFuncMap map[string]*shuffle.Shuffle) forward.GoWhere {
	getToBufferPartition := GetPartitionedBufferName()
	fsd := forward.GoWhere(func(keys []string, tags []string) ([]forward.VertexBuffer, error) {
		var result []forward.VertexBuffer

		if sharedutil.StringSliceContains(tags, dfv1.MessageTagDrop) {
			return result, nil
		}

		for _, edge := range sp.VertexInstance.Vertex.Spec.ToEdges {
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
						ToVertexPartition: getToBufferPartition(edge.To, edge.GetToVertexPartitions()),
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
							ToVertexPartition: getToBufferPartition(edge.To, edge.GetToVertexPartitions()),
						})
					}
				}
			}
		}
		return result, nil
	})
	return fsd
}

func GetPartitionedBufferName() func(toVertex string, toVertexPartitionCount int) int32 {
	messagePerPartitionMap := make(map[string]int)
	return func(toVertex string, toVertexPartitionCount int) int32 {
		vertexPartition := (messagePerPartitionMap[toVertex] + 1) % toVertexPartitionCount
		messagePerPartitionMap[toVertex] = vertexPartition
		return int32(vertexPartition)
	}
}
