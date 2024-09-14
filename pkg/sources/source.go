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
	"os"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/forwarder"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/stores/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/sdkclient"
	"github.com/numaproj/numaflow/pkg/sdkclient/serverinfo"
	sourceclient "github.com/numaproj/numaflow/pkg/sdkclient/source"
	"github.com/numaproj/numaflow/pkg/sdkclient/sourcetransformer"
	"github.com/numaproj/numaflow/pkg/shared/callback"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/shuffle"
	sourceforward "github.com/numaproj/numaflow/pkg/sources/forward"
	"github.com/numaproj/numaflow/pkg/sources/generator"
	"github.com/numaproj/numaflow/pkg/sources/http"
	jetstreamsrc "github.com/numaproj/numaflow/pkg/sources/jetstream"
	"github.com/numaproj/numaflow/pkg/sources/kafka"
	"github.com/numaproj/numaflow/pkg/sources/nats"
	"github.com/numaproj/numaflow/pkg/sources/sourcer"
	"github.com/numaproj/numaflow/pkg/sources/transformer"
	"github.com/numaproj/numaflow/pkg/sources/udsource"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"github.com/numaproj/numaflow/pkg/watermark/store"
	"github.com/numaproj/numaflow/pkg/watermark/wmb"
)

type SourceProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (sp *SourceProcessor) Start(ctx context.Context) error {
	var (
		sourcePublisherStores, _ = store.BuildNoOpWatermarkStore()
		sourceWmStores           = make(map[string]store.WatermarkStore)
		toVertexWatermarkStores  = make(map[string]store.WatermarkStore)
		log                      = logging.FromContext(ctx)
		writersMap               = make(map[string][]isb.BufferWriter)
		srcTransformerGRPCClient *transformer.GRPCBasedTransformer
		sourceReader             sourcer.SourceReader
		healthCheckers           []metrics.HealthChecker
		idleManager              wmb.IdleManager
		pipelineName             = sp.VertexInstance.Vertex.Spec.PipelineName
		vertexName               = sp.VertexInstance.Vertex.Spec.Name
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// watermark variables no-op initialization
	// create a no op fetcher
	fetchWatermark, _ := generic.BuildNoOpSourceWatermarkProgressors(sp.VertexInstance.Vertex.GetToBuffers())
	// create no op publisher stores
	for _, e := range sp.VertexInstance.Vertex.Spec.ToEdges {
		toVertexWatermarkStores[e.To], _ = store.BuildNoOpWatermarkStore()
	}
	idleManager = wmb.NewNoOpIdleManager()

	switch sp.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		for _, e := range sp.VertexInstance.Vertex.Spec.ToEdges {
			writeOpts := []redisclient.Option{
				redisclient.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
			}
			if x := e.ToVertexLimits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, redisclient.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.ToVertexLimits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, redisclient.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			partitionedBuffers := dfv1.GenerateBufferNames(sp.VertexInstance.Vertex.Namespace, sp.VertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitionCount())
			var bufferWriters []isb.BufferWriter
			// create a writer for each partition.
			for partitionIdx, partition := range partitionedBuffers {
				group := partition + "-group"
				redisClient := redisclient.NewInClusterRedisClient()
				writer := redisisb.NewBufferWrite(ctx, redisClient, partition, group, int32(partitionIdx), writeOpts...)
				bufferWriters = append(bufferWriters, writer)
			}
			writersMap[e.To] = bufferWriters
		}
	case dfv1.ISBSvcTypeJetStream:

		// create a new NATS client pool
		natsClientPool, err := jsclient.NewClientPool(ctx, jsclient.WithClientPoolSize(2))
		if err != nil {
			return fmt.Errorf("failed to create a new NATS client pool: %w", err)
		}
		defer natsClientPool.CloseAll()

		for _, e := range sp.VertexInstance.Vertex.Spec.ToEdges {
			writeOpts := []jetstreamisb.WriteOption{
				jetstreamisb.WithBufferFullWritingStrategy(e.BufferFullWritingStrategy()),
			}
			if x := e.ToVertexLimits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.ToVertexLimits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			var bufferWriters []isb.BufferWriter
			partitionedBuffers := dfv1.GenerateBufferNames(sp.VertexInstance.Vertex.Namespace, sp.VertexInstance.Vertex.Spec.PipelineName, e.To, e.GetToVertexPartitionCount())
			// create a writer for each partition.
			for partitionIdx, partition := range partitionedBuffers {
				streamName := isbsvc.JetStreamName(partition)
				jetStreamClient := natsClientPool.NextAvailableClient()
				writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jetStreamClient, partition, streamName, streamName, int32(partitionIdx), writeOpts...)
				if err != nil {
					return err
				}
				bufferWriters = append(bufferWriters, writer)
			}
			writersMap[e.To] = bufferWriters
		}

		// created watermark related components only if watermark is enabled
		// otherwise no op will be used
		if !sp.VertexInstance.Vertex.Spec.Watermark.Disabled {
			// build watermark stores for from vertex
			sourceWmStores, err = jetstream.BuildFromVertexWatermarkStores(ctx, sp.VertexInstance, natsClientPool.NextAvailableClient())
			if err != nil {
				return fmt.Errorf("failed to build watermark stores: %w", err)
			}

			// create watermark fetcher using watermark stores of from vertex
			fetchWatermark = fetch.NewSourceFetcher(ctx, sourceWmStores[sp.VertexInstance.Vertex.Name], fetch.WithIsSource(true))

			// build watermark stores for to-vertex
			toVertexWatermarkStores, err = jetstream.BuildToVertexWatermarkStores(ctx, sp.VertexInstance, natsClientPool.NextAvailableClient())
			if err != nil {
				return err
			}

			// build watermark stores for sourceReader (we publish twice for sourceReader)
			sourcePublisherStores, err = jetstream.BuildSourcePublisherStores(ctx, sp.VertexInstance, natsClientPool.NextAvailableClient())
			if err != nil {
				return err
			}
			idleManager, _ = wmb.NewIdleManager(1, len(writersMap))
		}
	default:
		return fmt.Errorf("unrecognized isb svc type %q", sp.ISBSvcType)
	}

	// Populate the shuffle function map
	// we need to shuffle the messages, because we can have a reduce vertex immediately after a sourceReader vertex.
	var toVertexPartitionMap = make(map[string]int)
	shuffleFuncMap := make(map[string]*shuffle.Shuffle)
	for _, edge := range sp.VertexInstance.Vertex.Spec.ToEdges {
		if edge.GetToVertexPartitionCount() > 1 {
			s := shuffle.NewShuffle(edge.To, edge.GetToVertexPartitionCount())
			shuffleFuncMap[fmt.Sprintf("%s:%s", edge.From, edge.To)] = s
		}
		toVertexPartitionMap[edge.To] = edge.GetToVertexPartitionCount()
	}

	maxMessageSize := sharedutil.LookupEnvIntOr(dfv1.EnvGRPCMaxMessageSize, sdkclient.DefaultGRPCMaxMessageSize)

	// if the sourceReader is a user-defined sourceReader, we create a gRPC client for it.
	var udsGRPCClient *udsource.GRPCBasedUDSource
	if sp.VertexInstance.Vertex.IsUDSource() {
		// Wait for server info to be ready
		serverInfo, err := serverinfo.SDKServerInfo(serverinfo.WithServerInfoFilePath(sdkclient.SourceServerInfoFile))
		if err != nil {
			return err
		}

		srcClient, err := sourceclient.New(serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize))
		if err != nil {
			return fmt.Errorf("failed to create a new gRPC client: %w", err)
		}

		udsGRPCClient, err = udsource.NewUDSgRPCBasedUDSource(sp.VertexInstance, srcClient)
		if err != nil {
			return fmt.Errorf("failed to create gRPC client, %w", err)
		}

		// Readiness check
		if err = udsGRPCClient.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on user-defined sourceReader readiness check, %w", err)
		}

		defer func() {
			err = udsGRPCClient.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close gRPC client conn", zap.Error(err))
			}
		}()
		healthCheckers = append(healthCheckers, udsGRPCClient)
	}

	var forwardOpts []sourceforward.Option

	if x := sp.VertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			forwardOpts = append(forwardOpts, sourceforward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}

	if sp.VertexInstance.Vertex.HasUDTransformer() {
		// Wait for server info to be ready
		serverInfo, err := serverinfo.SDKServerInfo(serverinfo.WithServerInfoFilePath(sdkclient.SourceTransformerServerInfoFile))
		if err != nil {
			return err
		}

		srcTransformerClient, err := sourcetransformer.New(serverInfo, sdkclient.WithMaxMessageSize(maxMessageSize))
		if err != nil {
			return fmt.Errorf("failed to create transformer gRPC client, %w", err)
		}

		srcTransformerGRPCClient = transformer.NewGRPCBasedTransformer(vertexName, srcTransformerClient)

		// Close the connection when we are done
		defer func() {
			err = srcTransformerGRPCClient.CloseConn(ctx)
			if err != nil {
				log.Warnw("Failed to close transformer gRPC client conn", zap.Error(err))
			}
		}()

		// Readiness check
		if err = srcTransformerGRPCClient.WaitUntilReady(ctx); err != nil {
			return fmt.Errorf("failed on user-defined transfomer readiness check, %w", err)
		}

		healthCheckers = append(healthCheckers, srcTransformerGRPCClient)
		forwardOpts = append(forwardOpts, sourceforward.WithTransformer(srcTransformerGRPCClient))
	}

	sourceReader, err := sp.createSourceReader(ctx, udsGRPCClient)
	if err != nil {
		return fmt.Errorf("failed to create source, error: %w", err)
	}

	// create a source watermark publisher
	sourceWmPublisher := publish.NewSourcePublish(ctx, pipelineName, vertexName, sourcePublisherStores, publish.WithDelay(sp.VertexInstance.Vertex.Spec.Watermark.GetMaxDelay()))

	// if the callback is enabled, create a callback publisher
	cbEnabled := sharedutil.LookupEnvBoolOr(dfv1.EnvCallbackEnabled, false)
	if cbEnabled {
		cbOpts := make([]callback.OptionFunc, 0)
		cbUrl := os.Getenv(dfv1.EnvCallbackURL)
		if cbUrl != "" {
			cbOpts = append(cbOpts, callback.WithCallbackURL(cbUrl))
		}
		cbPublisher := callback.NewUploader(ctx, vertexName, pipelineName, cbOpts...)
		forwardOpts = append(forwardOpts, sourceforward.WithCallbackUploader(cbPublisher))
	}

	// create source data forwarder
	var sourceForwarder *sourceforward.DataForward
	if sp.VertexInstance.Vertex.HasUDTransformer() {
		sourceForwarder, err = sourceforward.NewDataForward(sp.VertexInstance, sourceReader, writersMap, sp.getTransformerGoWhereDecider(shuffleFuncMap), fetchWatermark, sourceWmPublisher, toVertexWatermarkStores, idleManager, forwardOpts...)
	} else {
		sourceForwarder, err = sourceforward.NewDataForward(sp.VertexInstance, sourceReader, writersMap, sp.getSourceGoWhereDecider(shuffleFuncMap), fetchWatermark, sourceWmPublisher, toVertexWatermarkStores, idleManager, forwardOpts...)
	}
	if err != nil {
		return fmt.Errorf("failed to create source forwarder, error: %w", err)
	}

	log.Infow("Start processing source messages", zap.String("isbs", string(sp.ISBSvcType)), zap.Any("to", sp.VertexInstance.Vertex.GetToBuffers()))
	stopped := sourceForwarder.Start()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			<-stopped
			log.Info("Source forwarder stopped, exiting...")
			return
		}
	}()

	metricsOpts := metrics.NewMetricsOptions(ctx, sp.VertexInstance.Vertex, healthCheckers, []isb.LagReader{sourceReader})
	ms := metrics.NewMetricsServer(sp.VertexInstance.Vertex, metricsOpts...)
	if shutdown, err := ms.Start(ctx); err != nil {
		return fmt.Errorf("failed to start metrics server, error: %w", err)
	} else {
		defer func() { _ = shutdown(context.Background()) }()
	}
	<-ctx.Done()
	log.Info("SIGTERM, exiting...")
	sourceForwarder.Stop()
	wg.Wait()

	// close all the sourceReader wm stores
	for _, wmStore := range sourceWmStores {
		_ = wmStore.Close()
	}

	// close all the to vertex wm stores
	for _, ws := range toVertexWatermarkStores {
		_ = ws.Close()
	}

	// close all the sourceReader publisher stores
	_ = sourcePublisherStores.Close()

	log.Info("Exited...")
	return nil
}

// createSourceReader is used to send the sourcer information
func (sp *SourceProcessor) createSourceReader(ctx context.Context, udsGRPCClient *udsource.GRPCBasedUDSource) (sourcer.SourceReader, error) {
	var readTimeout time.Duration
	if l := sp.VertexInstance.Vertex.Spec.Limits; l != nil && l.ReadTimeout != nil {
		readTimeout = l.ReadTimeout.Duration
	}

	src := sp.VertexInstance.Vertex.Spec.Source
	if x := src.UDSource; x != nil && udsGRPCClient != nil {
		return udsource.NewUserDefinedSource(ctx, sp.VertexInstance, udsGRPCClient, udsource.WithReadTimeout(readTimeout))
	} else if x := src.Generator; x != nil {
		return generator.NewMemGen(ctx, sp.VertexInstance, generator.WithReadTimeout(readTimeout))
	} else if x := src.Kafka; x != nil {
		return kafka.NewKafkaSource(ctx, sp.VertexInstance, kafka.NewConsumerHandler(dfv1.DefaultKafkaHandlerChannelSize), kafka.WithReadTimeOut(readTimeout), kafka.WithGroupName(x.ConsumerGroupName))
	} else if x := src.HTTP; x != nil {
		return http.NewHttpSource(ctx, sp.VertexInstance, http.WithReadTimeout(readTimeout))
	} else if x := src.Nats; x != nil {
		return nats.New(ctx, sp.VertexInstance, nats.WithReadTimeout(readTimeout))
	} else if x := src.JetStream; x != nil {
		return jetstreamsrc.New(ctx, sp.VertexInstance, jetstreamsrc.WithReadTimeout(readTimeout))
	} else if x := src.Serving; x != nil {
		return jetstreamsrc.New(ctx, sp.VertexInstance, jetstreamsrc.WithReadTimeout(readTimeout), jetstreamsrc.WithServingEnabled())
	}
	return nil, fmt.Errorf("invalid source spec")
}

func (sp *SourceProcessor) getSourceGoWhereDecider(shuffleFuncMap map[string]*shuffle.Shuffle) forwarder.GoWhere {
	// create the conditional forwarder
	conditionalForwarder := forwarder.GoWhere(func(keys []string, tags []string, msgId string) ([]forwarder.VertexBuffer, error) {
		var result []forwarder.VertexBuffer

		// Iterate through the edges
		for _, edge := range sp.VertexInstance.Vertex.Spec.ToEdges {
			// if the edge has more than one partition, shuffle the message
			// else forward the message to the default partition
			partitionIdx := isb.DefaultPartitionIdx
			if edge.GetToVertexPartitionCount() > 1 {
				edgeKey := edge.From + ":" + edge.To
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

		return result, nil
	})
	return conditionalForwarder
}

func (sp *SourceProcessor) getTransformerGoWhereDecider(shuffleFuncMap map[string]*shuffle.Shuffle) forwarder.GoWhere {
	// create the conditional forwarder
	conditionalForwarder := forwarder.GoWhere(func(keys []string, tags []string, msgId string) ([]forwarder.VertexBuffer, error) {
		var result []forwarder.VertexBuffer

		// Drop message if it contains the special tag
		if sharedutil.StringSliceContains(tags, dfv1.MessageTagDrop) {
			metrics.UserDroppedMessages.With(map[string]string{
				metrics.LabelVertex:             sp.VertexInstance.Vertex.Spec.Name,
				metrics.LabelPipeline:           sp.VertexInstance.Vertex.Spec.PipelineName,
				metrics.LabelVertexType:         string(dfv1.VertexTypeSource),
				metrics.LabelVertexReplicaIndex: strconv.Itoa(int(sp.VertexInstance.Replica)),
			}).Inc()

			return result, nil
		}

		// Iterate through the edges
		for _, edge := range sp.VertexInstance.Vertex.Spec.ToEdges {
			// Condition to proceed for forwarding message: No conditions on edge, or message tags match edge conditions
			proceed := edge.Conditions == nil || edge.Conditions.Tags == nil || len(edge.Conditions.Tags.Values) == 0 || sharedutil.CompareSlice(edge.Conditions.Tags.GetOperator(), tags, edge.Conditions.Tags.Values)

			if proceed {
				// if the edge has more than one partition, shuffle the message
				// else forward the message to the default partition
				partitionIdx := isb.DefaultPartitionIdx
				if edge.GetToVertexPartitionCount() > 1 {
					edgeKey := edge.From + ":" + edge.To
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
	return conditionalForwarder
}
