package udf

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"

	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"

	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/redis"
	"github.com/numaproj/numaflow/pkg/metrics"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/udf/applier"
	"github.com/numaproj/numaflow/pkg/watermark/fetch"
	"github.com/numaproj/numaflow/pkg/watermark/publish"
)

type UDFProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *UDFProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var reader isb.BufferReader
	var err error
	fromBufferName := u.VertexInstance.Vertex.GetFromBuffers()[0].Name
	toBuffers := u.VertexInstance.Vertex.GetToBuffers()
	writers := make(map[string]isb.BufferWriter)

	// watermark variables
	var fetchWatermark fetch.Fetcher = nil
	var publishWatermark map[string]publish.Publisher = nil

	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		redisClient := redisclient.NewInClusterRedisClient()
		fromGroup := fromBufferName + "-group"
		readerOpts := []redisisb.Option{}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
			readerOpts = append(readerOpts, redisisb.WithReadTimeOut(x.ReadTimeout.Duration))
		}
		consumer := fmt.Sprintf("%s-%v", u.VertexInstance.Vertex.Name, u.VertexInstance.Replica)
		reader = redisisb.NewBufferRead(ctx, redisClient, fromBufferName, fromGroup, consumer, readerOpts...)
		for _, e := range u.VertexInstance.Vertex.Spec.ToEdges {

			writeOpts := []redisisb.Option{}
			if x := e.Limits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, redisisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.Limits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, redisisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			buffer := dfv1.GenerateEdgeBufferName(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, e.From, e.To)
			writer := redisisb.NewBufferWrite(ctx, redisClient, buffer, buffer+"-group", writeOpts...)
			writers[buffer] = writer
		}
	case dfv1.ISBSvcTypeJetStream:

		fromStreamName := fmt.Sprintf("%s-%s", u.VertexInstance.Vertex.Spec.PipelineName, fromBufferName)
		readOptions := []jetstreamisb.ReadOption{
			jetstreamisb.WithUsingAckInfoAsRate(true),
		}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
			readOptions = append(readOptions, jetstreamisb.WithReadTimeOut(x.ReadTimeout.Duration))
		}
		reader, err = jetstreamisb.NewJetStreamBufferReader(ctx, jsclient.NewInClusterJetStreamClient(), fromBufferName, fromStreamName, fromStreamName, readOptions...)
		if err != nil {
			return err
		}

		// build watermark progressors
		fetchWatermark, publishWatermark, err = jetstream.BuildJetStreamWatermarkProgressors(ctx, u.VertexInstance)
		if err != nil {
			return err
		}

		for _, e := range u.VertexInstance.Vertex.Spec.ToEdges {
			writeOpts := []jetstreamisb.WriteOption{}
			if x := e.Limits; x != nil && x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x := e.Limits; x != nil && x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
			buffer := dfv1.GenerateEdgeBufferName(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, e.From, e.To)
			streamName := fmt.Sprintf("%s-%s", u.VertexInstance.Vertex.Spec.PipelineName, buffer)
			writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jsclient.NewInClusterJetStreamClient(), buffer, streamName, streamName, writeOpts...)
			if err != nil {
				return err
			}
			writers[buffer] = writer
		}
	default:
		return fmt.Errorf("unrecognized isbs type %q", u.ISBSvcType)
	}

	conditionalForwarder := forward.GoWhere(func(key string) ([]string, error) {
		result := []string{}
		_key := string(key)
		if _key == dfv1.MessageKeyAll || _key == dfv1.MessageKeyDrop {
			result = append(result, _key)
			return result, nil
		}
		for _, to := range u.VertexInstance.Vertex.Spec.ToEdges {
			// If returned key is not "ALL" or "DROP", and there's no conditions defined in the edge,
			// treat it as "ALL"?
			if to.Conditions == nil || len(to.Conditions.KeyIn) == 0 || sharedutil.StringSliceContains(to.Conditions.KeyIn, _key) {
				result = append(result, dfv1.GenerateEdgeBufferName(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, to.From, to.To))
			}
		}
		return result, nil
	})

	udfHandler := applier.NewUDSHTTPBasedUDF(dfv1.PathVarRun+"/udf.sock", applier.WithHTTPClientTimeout(120*time.Second))
	// Readiness check
	if err := udfHandler.WaitUntilReady(ctx); err != nil {
		return fmt.Errorf("failed on UDF readiness check, %w", err)
	}
	log.Infow("Start processing udf messages", zap.String("isbs", string(u.ISBSvcType)), zap.String("from", fromBufferName), zap.Any("to", toBuffers))
	opts := []forward.Option{forward.WithLogger(log)}
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
	forwarder.Stop()
	wg.Wait()
	log.Info("Exited...")
	return nil
}
