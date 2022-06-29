package sources

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sources/generator"
	"github.com/numaproj/numaflow/pkg/sources/http"
	"github.com/numaproj/numaflow/pkg/sources/kafka"
)

type SourceProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *SourceProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var writers []isb.BufferWriter
	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		for _, e := range u.VertexInstance.Vertex.Spec.ToEdges {
			writeOpts := []redisisb.Option{}
			if x := e.Limits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, redisisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.Limits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, redisisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			buffer := dfv1.GenerateEdgeBufferName(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, e.From, e.To)
			group := buffer + "-group"
			redisClient := clients.NewInClusterRedisClient()
			writer := redisisb.NewBufferWrite(ctx, redisClient, buffer, group, writeOpts...)
			writers = append(writers, writer)
		}
	case dfv1.ISBSvcTypeJetStream:
		for _, e := range u.VertexInstance.Vertex.Spec.ToEdges {
			writeOpts := []jetstreamisb.WriteOption{
				jetstreamisb.WithUsingWriteInfoAsRate(true),
			}
			if x := u.VertexInstance.Vertex.Spec.Scale.LookbackSeconds; x != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithWriteRateLookbackSeconds(int64(*x)))
			}
			if x := e.Limits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.Limits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			buffer := dfv1.GenerateEdgeBufferName(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, e.From, e.To)
			streamName := fmt.Sprintf("%s-%s", u.VertexInstance.Vertex.Spec.PipelineName, buffer)
			jetStreamClient := clients.NewInClusterJetStreamClient()
			writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jetStreamClient, buffer, streamName, streamName, writeOpts...)
			if err != nil {
				return err
			}
			writers = append(writers, writer)
		}
	default:
		return fmt.Errorf("unrecognized isb svc type %q", u.ISBSvcType)
	}

	sourcer, err := u.getSourcer(writers, log)
	if err != nil {
		return fmt.Errorf("failed to find a sourcer, error: %w", err)
	}
	log.Infow("Start processing source messages", zap.String("isbs", string(u.ISBSvcType)), zap.Any("to", u.VertexInstance.Vertex.GetToBuffers()))
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

	metricsOpts := []metrics.Option{}
	if x, ok := sourcer.(isb.LagReader); ok {
		metricsOpts = append(metricsOpts, metrics.WithLagReader(x))
		if s := u.VertexInstance.Vertex.Spec.Scale.LookbackSeconds; s != nil {
			metricsOpts = append(metricsOpts, metrics.WithPendingLookbackSeconds(int64(*s)))
		}
	}
	if x, ok := writers[0].(isb.Ratable); ok { // Only need to use the rate of one of the writer
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
	sourcer.Stop()
	wg.Wait()
	log.Info("Exited...")
	return nil
}

// getSourcer is used to send the sourcer information
func (u *SourceProcessor) getSourcer(writers []isb.BufferWriter, logger *zap.SugaredLogger) (Sourcer, error) {
	src := u.VertexInstance.Vertex.Spec.Source
	if x := src.Generator; x != nil {
		return generator.NewMemGen(u.VertexInstance, int(*x.RPU), *x.MsgSize, x.Duration.Duration, writers, generator.WithLogger(logger))
	} else if x := src.Kafka; x != nil {
		return kafka.NewKafkaSource(u.VertexInstance.Vertex, writers, kafka.WithGroupName(x.ConsumerGroupName), kafka.WithLogger(logger))
	} else if x := src.HTTP; x != nil {
		return http.New(u.VertexInstance.Vertex, writers, http.WithLogger(logger))
	}
	return nil, fmt.Errorf("invalid source spec")
}
