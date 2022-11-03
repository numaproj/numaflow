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

package sinks

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
	kafkasink "github.com/numaproj/numaflow/pkg/sinks/kafka"
	logsink "github.com/numaproj/numaflow/pkg/sinks/logger"
	udsink "github.com/numaproj/numaflow/pkg/sinks/udsink"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
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
	// publishWatermark is a map representing a progressor per edge, we are initializing them to a no-op progressor
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(u.VertexInstance.Vertex.GetToBuffers()))

	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		redisClient := redisclient.NewInClusterRedisClient()
		fromGroup := fromBufferName + "-group"
		consumer := fmt.Sprintf("%s-%v", u.VertexInstance.Vertex.Name, u.VertexInstance.Replica)
		readOptions := []redisisb.Option{}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
			readOptions = append(readOptions, redisisb.WithReadTimeOut(x.ReadTimeout.Duration))
		}
		reader = redisisb.NewBufferRead(ctx, redisClient, fromBufferName, fromGroup, consumer, readOptions...)
	case dfv1.ISBSvcTypeJetStream:
		streamName := isbsvc.JetStreamName(u.VertexInstance.Vertex.Spec.PipelineName, fromBufferName)
		readOptions := []jetstreamisb.ReadOption{
			jetstreamisb.WithUsingAckInfoAsRate(true),
		}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
			readOptions = append(readOptions, jetstreamisb.WithReadTimeOut(x.ReadTimeout.Duration))
		}
		// build watermark progressors
		fetchWatermark, publishWatermark, err = jetstream.BuildWatermarkProgressors(ctx, u.VertexInstance)
		if err != nil {
			return err
		}

		jetStreamClient := jsclient.NewInClusterJetStreamClient()
		reader, err = jetstreamisb.NewJetStreamBufferReader(ctx, jetStreamClient, fromBufferName, streamName, streamName, readOptions...)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unrecognized isb svc type %q", u.ISBSvcType)
	}

	sinker, err := u.getSinker(reader, log, fetchWatermark, publishWatermark)
	if err != nil {
		return fmt.Errorf("failed to find a sink, errpr: %w", err)
	}

	log.Infow("Start processing sink messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBufferName))
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
	if x, ok := sinker.(*udsink.UserDefinedSink); ok {
		metricsOpts = append(metricsOpts, metrics.WithHealthCheckExecutor(x.IsHealthy))
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
	if x := sink.Log; x != nil {
		return logsink.NewToLog(u.VertexInstance.Vertex, reader, fetchWM, publishWM, logsink.WithLogger(logger))
	} else if x := sink.Kafka; x != nil {
		return kafkasink.NewToKafka(u.VertexInstance.Vertex, reader, fetchWM, publishWM, kafkasink.WithLogger(logger))
	} else if x := sink.UDSink; x != nil {
		return udsink.NewUserDefinedSink(u.VertexInstance.Vertex, reader, fetchWM, publishWM, udsink.WithLogger(logger))
	}
	return nil, fmt.Errorf("invalid sink spec")
}
