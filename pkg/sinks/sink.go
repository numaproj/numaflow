package sinks

import (
	"context"
	"fmt"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
	"sync"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/metrics"

	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	kafkasink "github.com/numaproj/numaflow/pkg/sinks/kafka"
	logsink "github.com/numaproj/numaflow/pkg/sinks/logger"
	udsink "github.com/numaproj/numaflow/pkg/sinks/udsink"
)

type SinkProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *SinkProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var reader isb.BufferReader
	var err error
	fromBufferName := u.VertexInstance.Vertex.GetFromBuffers()[0].Name

	// watermark variables no-op initialization
	var fetchWatermark fetch.Fetcher = generic.NewNoOpWMProgressor()
	// publishWatermark is a map representing a progressor per edge, we are initializing them to a no-op progressor
	publishWatermark := make(map[string]publish.Publisher)
	for _, buffer := range u.VertexInstance.Vertex.GetToBuffers() {
		publishWatermark[buffer.Name] = generic.NewNoOpWMProgressor()
	}

	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		redisClient := clients.NewInClusterRedisClient()
		fromGroup := fromBufferName + "-group"
		consumer := fmt.Sprintf("%s-%v", u.VertexInstance.Vertex.Name, u.VertexInstance.Replica)
		readOptions := []redisisb.Option{}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
			readOptions = append(readOptions, redisisb.WithReadTimeOut(x.ReadTimeout.Duration))
		}
		reader = redisisb.NewBufferRead(ctx, redisClient, fromBufferName, fromGroup, consumer, readOptions...)
	case dfv1.ISBSvcTypeJetStream:
		streamName := fmt.Sprintf("%s-%s", u.VertexInstance.Vertex.Spec.PipelineName, fromBufferName)
		readOptions := []jetstreamisb.ReadOption{
			jetstreamisb.WithUsingAckInfoAsRate(true),
		}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
			readOptions = append(readOptions, jetstreamisb.WithReadTimeOut(x.ReadTimeout.Duration))
		}
		// build watermark progressors
		fetchWatermark, publishWatermark = generic.BuildJetStreamWatermarkProgressors(ctx, u.VertexInstance)

		jetStreamClient := clients.NewInClusterJetStreamClient()
		reader, err = jetstreamisb.NewJetStreamBufferReader(ctx, jetStreamClient, fromBufferName, streamName, streamName, readOptions...)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbs type %q", u.ISBSvcType)
	}

	sinker, err := u.getSinker(reader, log, fetchWatermark, publishWatermark)
	if err != nil {
		return fmt.Errorf("failed to find a sink, errpr: %w", err)
	}

	log.Infow("Start processing sink messages", zap.String("isbs", string(u.ISBSvcType)), zap.String("from", fromBufferName))
	stopped := sinker.Start()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			<-stopped
			log.Info("Sinker stopped, exiting sink processor...")
			return
		}
	}()

	metricsOpts := []metrics.Option{metrics.WithLookbackSeconds(int64(u.VertexInstance.Vertex.Spec.Scale.GetLookbackSeconds()))}
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
	sinker.Stop()
	wg.Wait()
	log.Info("Exited...")
	return nil
}

// getSinker takes in the logger from the parent context
func (u *SinkProcessor) getSinker(reader isb.BufferReader, logger *zap.SugaredLogger, fetchWM fetch.Fetcher, publishWM map[string]publish.Publisher) (Sinker, error) {
	sink := u.VertexInstance.Vertex.Spec.Sink
	// TODO: add watermark
	if x := sink.Log; x != nil {
		return logsink.NewToLog(u.VertexInstance.Vertex, reader, fetchWM, publishWM, logsink.WithLogger(logger))
	} else if x := sink.Kafka; x != nil {
		return kafkasink.NewToKafka(u.VertexInstance.Vertex, reader, fetchWM, publishWM, kafkasink.WithLogger(logger))
	} else if x := sink.UDSink; x != nil {
		return udsink.NewUserDefinedSink(u.VertexInstance.Vertex, reader, fetchWM, publishWM, udsink.WithLogger(logger))
	}
	return nil, fmt.Errorf("invalid sink spec")
}
