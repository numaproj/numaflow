package sinks

import (
	"context"
	"fmt"
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
	ISBSvcType dfv1.ISBSvcType
	Vertex     *dfv1.Vertex
	Hostname   string
	Replica    int
}

func (u *SinkProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var reader isb.BufferReader
	var err error
	fromBufferName := u.Vertex.GetFromBuffers()[0]
	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		redisClient := clients.NewInClusterRedisClient()
		fromGroup := fromBufferName + "-group"
		consumer := fmt.Sprintf("%s-%v", u.Vertex.Name, u.Replica)
		reader = redisisb.NewBufferRead(ctx, redisClient, fromBufferName, fromGroup, consumer)
	case dfv1.ISBSvcTypeJetStream:
		streamName := fmt.Sprintf("%s-%s", u.Vertex.Spec.PipelineName, fromBufferName)
		jetStreamClient := clients.NewInClusterJetStreamClient()
		reader, err = jetstreamisb.NewJetStreamBufferReader(ctx, jetStreamClient, fromBufferName, streamName, streamName)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized isbs type %q", u.ISBSvcType)
	}

	sinker, err := u.getSinker(reader, log)
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

	if shutdown, err := metrics.StartMetricsServer(ctx); err != nil {
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
func (u *SinkProcessor) getSinker(reader isb.BufferReader, logger *zap.SugaredLogger) (Sinker, error) {
	sink := u.Vertex.Spec.Sink
	if x := sink.Log; x != nil {
		return logsink.NewToLog(u.Vertex, reader, logsink.WithLogger(logger))
	} else if x := sink.Kafka; x != nil {
		return kafkasink.NewToKafka(u.Vertex, reader, kafkasink.WithLogger(logger))
	} else if x := sink.UDSink; x != nil {
		return udsink.NewUserDefinedSink(u.Vertex, reader, udsink.WithLogger(logger))
	}
	return nil, fmt.Errorf("invalid sink spec")
}
