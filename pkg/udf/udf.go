package udf

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/numaproj/numaflow/pkg/watermark/progress"
	"go.uber.org/zap"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isb"
	"github.com/numaproj/numaflow/pkg/isb/forward"
	jetstreamisb "github.com/numaproj/numaflow/pkg/isb/jetstream"
	redisisb "github.com/numaproj/numaflow/pkg/isb/redis"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/metrics"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/udf/applier"
)

type UDFProcessor struct {
	ISBSvcType dfv1.ISBSvcType
	Vertex     *dfv1.Vertex
	Hostname   string
	Replica    int
}

func (u *UDFProcessor) Start(ctx context.Context) error {
	log := logging.FromContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var reader isb.BufferReader
	var err error
	fromBufferName := u.Vertex.GetFromBuffers()[0].Name
	toBuffers := u.Vertex.GetToBuffers()
	writers := make(map[string]isb.BufferWriter)
	switch u.ISBSvcType {
	case dfv1.ISBSvcTypeRedis:
		redisClient := clients.NewInClusterRedisClient()
		fromGroup := fromBufferName + "-group"
		consumer := fmt.Sprintf("%s-%v", u.Vertex.Name, u.Replica)
		reader = redisisb.NewBufferRead(ctx, redisClient, fromBufferName, fromGroup, consumer)
		writeOpts := []redisisb.Option{}
		if x := u.Vertex.Spec.Limits; x != nil {
			if x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, redisisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, redisisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
		}
		for _, buffer := range toBuffers {
			writer := redisisb.NewBufferWrite(ctx, redisClient, buffer.Name, buffer.Name+"-group", writeOpts...)
			writers[buffer.Name] = writer
		}
	case dfv1.ISBSvcTypeJetStream:
		fromStreamName := fmt.Sprintf("%s-%s", u.Vertex.Spec.PipelineName, fromBufferName)
		jetStreamClient := clients.NewInClusterJetStreamClient()
		reader, err = jetstreamisb.NewJetStreamBufferReader(ctx, jetStreamClient, fromBufferName, fromStreamName, fromStreamName)
		if err != nil {
			return err
		}
		writeOpts := []jetstreamisb.WriteOption{}
		if x := u.Vertex.Spec.Limits; x != nil {
			if x.BufferMaxLength != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithMaxLength(int64(*x.BufferMaxLength)))
			}
			if x.BufferUsageLimit != nil {
				writeOpts = append(writeOpts, jetstreamisb.WithBufferUsageLimit(float64(*x.BufferUsageLimit)/100))
			}
		}
		for _, buffer := range toBuffers {
			streamName := fmt.Sprintf("%s-%s", u.Vertex.Spec.PipelineName, buffer.Name)
			writer, err := jetstreamisb.NewJetStreamBufferWriter(ctx, jetStreamClient, buffer.Name, streamName, streamName, writeOpts...)
			if err != nil {
				return err
			}
			writers[buffer.Name] = writer
		}
	default:
		return fmt.Errorf("unrecognized isbs type %q", u.ISBSvcType)
	}

	conditionalForwarder := forward.GoWhere(func(key []byte) ([]string, error) {
		result := []string{}
		_key := string(key)
		if _key == dfv1.MessageKeyAll || _key == dfv1.MessageKeyDrop {
			result = append(result, _key)
			return result, nil
		}
		for _, to := range u.Vertex.Spec.ToVertices {
			// If returned key is not "ALL" or "DROP", and there's no conditions defined in the edge,
			// treat it as "ALL"?
			if to.Conditions == nil || len(to.Conditions.KeyIn) == 0 || sharedutil.StringSliceContains(to.Conditions.KeyIn, _key) {
				result = append(result, dfv1.GenerateEdgeBufferName(u.Vertex.Namespace, u.Vertex.Spec.PipelineName, u.Vertex.Spec.Name, to.Name))
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
	if x := u.Vertex.Spec.Limits; x != nil {
		if x.ReadBatchSize != nil {
			opts = append(opts, forward.WithReadBatchSize(int64(*x.ReadBatchSize)))
		}
		if x.UDFWorkers != nil {
			opts = append(opts, forward.WithUDFConcurrency(int(*x.UDFWorkers)))
		}
	}

	var wmProgressor progress.Progressor = nil
	if val, ok := os.LookupEnv(dfv1.EnvWatermarkOn); ok && val == "true" {
		js, err := progress.GetJetStreamConnection(ctx)
		if err != nil {
			return err
		}
		// TODO: remove this once bucket creation has been moved to controller
		err = progress.CreateProcessorBucketIfMissing(fmt.Sprintf("%s_PROCESSORS", progress.GetPublishKeySpace(u.Vertex)), js)
		if err != nil {
			return err
		}
		wmProgressor = progress.NewGenericProgress(ctx, fmt.Sprintf("%s-%d", u.Vertex.Name, u.Replica), progress.GetFetchKeyspace(u.Vertex), progress.GetPublishKeySpace(u.Vertex), js)
	}

	forwarder, err := forward.NewInterStepDataForward(u.Vertex, reader, writers, conditionalForwarder, udfHandler, wmProgressor, opts...)
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

	if shutdown, err := metrics.StartMetricsServer(ctx); err != nil {
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
