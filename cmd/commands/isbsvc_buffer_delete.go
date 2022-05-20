package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func NewISBSvcBufferDeleteCommand() *cobra.Command {
	var (
		isbSvcType string
		buffers    []string
	)

	command := &cobra.Command{
		Use:   "isbsvc-buffer-delete",
		Short: "Delete ISB Service buffers",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.NewLogger().Named("isbsvc-buffer-delete")
			if len(buffers) == 0 {
				cmd.HelpFunc()(cmd, args)
				return fmt.Errorf("buffer not supplied")
			}
			pipelineName, defined := os.LookupEnv(v1alpha1.EnvPipelineName)
			if !defined {
				return fmt.Errorf("required environment variable '%s' not defined", v1alpha1.EnvPipelineName)
			}
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
			default:
				cmd.HelpFunc()(cmd, args)
				return fmt.Errorf("unsupported isb service type %q", isbSvcType)
			}
			if err = isbsClient.DeleteBuffers(ctx, buffers); err != nil {
				logger.Errorw("Failed buffer deletion.", zap.Error(err))
				return err
			}
			logger.Info("Deleted Buffers successfully")
			return nil
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "jetstream", "ISB Service type, e.g. jetstream")
	command.Flags().StringSliceVar(&buffers, "buffers", []string{}, "Buffers to delete") // --buffers=xxa,xxb --buffers=xxc
	return command
}
