package udf

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/stores/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/stores/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/pbq"
	"github.com/numaproj/numaflow/pkg/pbq/store"
	"github.com/numaproj/numaflow/pkg/reduce"
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/jetstream"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/udf/function"
	"github.com/numaproj/numaflow/pkg/watermark/generic"
	"github.com/numaproj/numaflow/pkg/watermark/generic/jetstream"
	"github.com/numaproj/numaflow/pkg/window"
	"github.com/numaproj/numaflow/pkg/window/strategy/fixed"
	"go.uber.org/zap"
)

type ReduceUDFProcessor struct {
	ISBSvcType     dfv1.ISBSvcType
	VertexInstance *dfv1.VertexInstance
}

func (u *ReduceUDFProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var windower window.Windower
	if x := u.VertexInstance.Vertex.Spec.UDF.GroupBy.Window.Fixed; x != nil {
		// TODO: check if Length is empty
		windower = fixed.NewFixed(x.Length.Duration)
	}
	if windower == nil {
		return fmt.Errorf("invalid window spec")
	}
	var reader isb.BufferReader
	var err error
	fromBuffers := u.VertexInstance.Vertex.GetFromBuffers()
	var fromBufferName string
	for _, b := range fromBuffers {
		if strings.HasSuffix(b.Name, fmt.Sprintf("-%d", u.VertexInstance.Replica)) {
			fromBufferName = b.Name
			break
		}
	}
	if len(fromBufferName) == 0 {
		return fmt.Errorf("can not find from buffer")
	}
	writers := make(map[string]isb.BufferWriter)
	// watermark variables
	fetchWatermark, publishWatermark := generic.BuildNoOpWatermarkProgressorsFromEdgeList(generic.GetBufferNameList(u.VertexInstance.Vertex.GetToBuffers()))
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
			buffers := dfv1.GenerateEdgeBufferNames(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, e)
			for _, buffer := range buffers {
				writer := redisisb.NewBufferWrite(ctx, redisClient, buffer, buffer+"-group", writeOpts...)
				writers[buffer] = writer
			}
		}
	case dfv1.ISBSvcTypeJetStream:
		fromStreamName := isbsvc.JetStreamName(u.VertexInstance.Vertex.Spec.PipelineName, fromBufferName)
		readOptions := []jetstreamisb.ReadOption{}
		if x := u.VertexInstance.Vertex.Spec.Limits; x != nil && x.ReadTimeout != nil {
			readOptions = append(readOptions, jetstreamisb.WithReadTimeOut(x.ReadTimeout.Duration))
		}
		reader, err = jetstreamisb.NewJetStreamBufferReader(ctx, jsclient.NewInClusterJetStreamClient(), fromBufferName, fromStreamName, fromStreamName, readOptions...)
		if err != nil {
			return err
		}

		// build watermark progressors
		fetchWatermark, publishWatermark, err = jetstream.BuildWatermarkProgressors(ctx, u.VertexInstance)
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
			buffers := dfv1.GenerateEdgeBufferNames(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, e)
			for _, buffer := range buffers {
				streamName := isbsvc.JetStreamName(u.VertexInstance.Vertex.Spec.PipelineName, buffer)
				writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jsclient.NewInClusterJetStreamClient(), buffer, streamName, streamName, writeOpts...)
				if err != nil {
					return err
				}
				writers[buffer] = writer
			}
		}
	default:
		return fmt.Errorf("unrecognized isbsvc type %q", u.ISBSvcType)
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
				result = append(result, dfv1.GenerateEdgeBufferNames(u.VertexInstance.Vertex.Namespace, u.VertexInstance.Vertex.Spec.PipelineName, to)...)
			}
		}
		return result, nil
	})

	log = log.With("protocol", "uds-grpc-reduce-udf")
	udfHandler, err := function.NewUDSGRPCBasedUDF()
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
	log.Infow("Start processing reduce udf messages", zap.String("isbsvc", string(u.ISBSvcType)), zap.String("from", fromBufferName))

	var pbqManager *pbq.Manager
	pbqManager, err = pbq.NewManager(ctx, pbq.WithPBQStoreOptions(store.WithPbqStoreType(dfv1.InMemoryType)))

	if err != nil {
		log.Errorw("Failed to create pbq manager", zap.Error(err))
		return fmt.Errorf("failed to create pbq manager, %w", err)
	}
	opts := []reduce.Option{}
	if x := u.VertexInstance.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			opts = append(opts, reduce.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
	}
	dataforwarder, err := reduce.NewDataForward(ctx, udfHandler, reader, writers, pbqManager, conditionalForwarder, fetchWatermark, publishWatermark, windower, opts...)
	if err != nil {
		return fmt.Errorf("failed get a new DataForward, %w", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		dataforwarder.Start(ctx)
		log.Info("Forwarder stopped, exiting reduce udf data processor...")
	}()

	metricsOpts := []metrics.Option{
		metrics.WithLookbackSeconds(int64(u.VertexInstance.Vertex.Spec.Scale.GetLookbackSeconds())),
		metrics.WithHealthCheckExecutor(func() error {
			cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			return udfHandler.WaitUntilReady(cctx)
		}),
	}
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
	wg.Wait()
	log.Info("Exited...")
	return nil
}
