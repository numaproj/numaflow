package commands

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func NewISBSvcBufferCreateCommand() *cobra.Command {

	var (
		isbSvcType string
		buffers    []string
	)

	command := &cobra.Command{
		Use:   "isbsvc-buffer-create",
		Short: "Create ISB Service buffers",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.NewLogger().Named("isbsvc-buffer-create")
			if len(buffers) == 0 {
				cmd.HelpFunc()(cmd, args)
				return fmt.Errorf("buffer list should not be empty")
			}
			pipelineName, defined := os.LookupEnv(v1alpha1.EnvPipelineName)
			if !defined {
				return fmt.Errorf("required environment variable '%s' not defined", v1alpha1.EnvPipelineName)
			}
			isbSvcConfig := &v1alpha1.BufferServiceConfig{}
			encodedBufferServiceConfig := os.Getenv(v1alpha1.EnvISBSvcConfig)
			if len(encodedBufferServiceConfig) > 0 {
				isbSvcConfigStr, err := base64.StdEncoding.DecodeString(encodedBufferServiceConfig)
				if err != nil {
					return fmt.Errorf("failed to decode ISB Svc config string, %w", err)
				}
				if err := json.Unmarshal(isbSvcConfigStr, isbSvcConfig); err != nil {
					return fmt.Errorf("failed to unmarshal ISB Svc config, %w", err)
				}
			}
			opts := []isbsvc.BufferCreateOption{}
			var isbsClient isbsvc.ISBService
			var err error
			ctx := logging.WithLogger(context.Background(), logger)
			switch v1alpha1.ISBSvcType(isbSvcType) {
			case v1alpha1.ISBSvcTypeRedis:
				isbsClient = isbsvc.NewISBRedisSvc(clients.NewInClusterRedisClient())
			case v1alpha1.ISBSvcTypeJetStream:
				isbsClient, err = isbsvc.NewISBJetStreamSvc(pipelineName)
				if err != nil {
					logger.Errorw("Failed to get a ISB Service client.", zap.Error(err))
					return err
				}
				opts = append(opts, isbsvc.WithBufferConfig(isbSvcConfig.JetStream.BufferConfig))
			default:
				cmd.HelpFunc()(cmd, args)
				return fmt.Errorf("unsupported isb service type %q", isbSvcType)
			}

			if err = isbsClient.CreateBuffers(ctx, buffers, opts...); err != nil {
				logger.Errorw("Failed buffer creation.", zap.Error(err))
				return err
			}
			logger.Info("Created buffers successfully")
			return nil
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "jetstream", "ISB Service type, e.g. jetstream")
	command.Flags().StringSliceVar(&buffers, "buffers", []string{}, "Buffers to create") // --buffers=xxa,xxb --buffers=xxc
	return command
}
