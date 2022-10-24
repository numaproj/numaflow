package sources

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/stores/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/metrics"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sources/generator"
	"github.com/numaproj/numaflow/pkg/sources/http"
	"github.com/numaproj/numaflow/pkg/sources/kafka"
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
	var writers []isb.BufferWriter

	// watermark variables no-op initialization
	// publishWatermark is a map representing a progressor per edge, we are initializing them to a no-op progressor
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(sp.VertexInstance.Vertex.GetToBuffers()))
	var sourcePublisherStores = store.BuildWatermarkStore(noop.NewKVNoOpStore(), noop.NewKVNoOpStore())
	var err error

	switch sp.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		for _, e := range sp.VertexInstance.Vertex.Spec.ToEdges {
			writeOpts := []redisisb.Option{}
			if x := e.Limits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, redisisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.Limits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, redisisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			buffers := dfv1.GenerateEdgeBufferNames(sp.VertexInstance.Vertex.Namespace, sp.VertexInstance.Vertex.Spec.PipelineName, e)
			for _, buffer := range buffers {
				group := buffer + "-group"
				redisClient := redisclient.NewInClusterRedisClient()
				writer := redisisb.NewBufferWrite(ctx, redisClient, buffer, group, writeOpts...)
				writers = append(writers, writer)
			}
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
				jetstreamisb.WithUsingWriteInfoAsRate(true),
			}
			if x := e.Limits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.Limits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			buffers := dfv1.GenerateEdgeBufferNames(sp.VertexInstance.Vertex.Namespace, sp.VertexInstance.Vertex.Spec.PipelineName, e)
			for _, buffer := range buffers {
				streamName := isbsvc.JetStreamName(sp.VertexInstance.Vertex.Spec.PipelineName, buffer)
				jetStreamClient := jsclient.NewInClusterJetStreamClient()
				writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jetStreamClient, buffer, streamName, streamName, writeOpts...)
				if err != nil {
					return err
				}
				writers = append(writers, writer)
			}
		}
	default:
		return fmt.Errorf("unrecognized isb svc type %q", sp.ISBSvcType)
	}

	sourcer, err := sp.getSourcer(writers, fetchWatermark, publishWatermark, sourcePublisherStores, log)
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

	metricsOpts := []metrics.Option{metrics.WithLookbackSeconds(int64(sp.VertexInstance.Vertex.Spec.Scale.GetLookbackSeconds()))}
	if x, ok := sourcer.(isb.LagReader); ok {
		metricsOpts = append(metricsOpts, metrics.WithLagReader(x))
	}
	if x, ok := writers[0].(isb.Ratable); ok { // Only need to use the rate of one of the writer
		metricsOpts = append(metricsOpts, metrics.WithRater(x))
	}
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
func (sp *SourceProcessor) getSourcer(writers []isb.BufferWriter, fetchWM fetch.Fetcher, publishWM map[string]publish.Publisher, publishWMStores store.WatermarkStorer, logger *zap.SugaredLogger) (Sourcer, error) {
	src := sp.VertexInstance.Vertex.Spec.Source

	if x := src.Generator; x != nil {
		readOptions := []generator.Option{
			generator.WithLogger(logger),
		}
		if l := sp.VertexInstance.Vertex.Spec.Limits; l != nil && l.ReadTimeout != nil {
			readOptions = append(readOptions, generator.WithReadTimeOut(l.ReadTimeout.Duration))
		}
		return generator.NewMemGen(sp.VertexInstance, writers, fetchWM, publishWM, publishWMStores, readOptions...)
	} else if x := src.Kafka; x != nil {
		readOptions := []kafka.Option{
			kafka.WithGroupName(x.ConsumerGroupName),
			kafka.WithLogger(logger),
		}
		if l := sp.VertexInstance.Vertex.Spec.Limits; l != nil && l.ReadTimeout != nil {
			readOptions = append(readOptions, kafka.WithReadTimeOut(l.ReadTimeout.Duration))
		}
		return kafka.NewKafkaSource(sp.VertexInstance, writers, fetchWM, publishWM, publishWMStores, readOptions...)
	} else if x := src.HTTP; x != nil {
		return http.New(sp.VertexInstance, writers, fetchWM, publishWM, publishWMStores, http.WithLogger(logger))
	}
	return nil, fmt.Errorf("invalid source spec")
}
