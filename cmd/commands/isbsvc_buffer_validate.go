package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	"github.com/numaproj/numaflow/pkg/isbsvc/clients"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

func NewISBSvcBufferValidateCommand() *cobra.Command {

	var (
		isbSvcType string
		buffers    map[string]string
	)

	command := &cobra.Command{
		Use:   "isbsvc-buffer-validate",
		Short: "Validate ISB Service buffers",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.NewLogger().Named("isbsvc-buffer-validate")
			if len(buffers) == 0 {
				cmd.HelpFunc()(cmd, args)
				return fmt.Errorf("buffer not supplied")
			}
			pipelineName, existing := os.LookupEnv(v1alpha1.EnvPipelineName)
			if !existing {
				return fmt.Errorf("environment variable %q not existing", v1alpha1.EnvPipelineName)
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
				return fmt.Errorf("unsupported isb service type")
			}
			bfs := []v1alpha1.Buffer{}
			for k, v := range buffers {
				bfs = append(bfs, v1alpha1.Buffer{Name: k, Type: v1alpha1.BufferType(v)})
			}
			if err := wait.ExponentialBackoffWithContext(ctx, sharedutil.DefaultRetryBackoff, func() (bool, error) {
				if err := isbsClient.ValidateBuffers(ctx, bfs); err != nil {
					logger.Errorw("Buffers validation failed, will retry if the limit is not reached", zap.Error(err))
					return false, nil
				}
				return true, nil
			}); err != nil {
				logger.Errorw("Failed buffer validation after retrying.", zap.Error(err))
				return err
			}
			logger.Info("Validate buffers successfully")
			return nil
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "jetstream", "ISB Service type, e.g. jetstream")
	command.Flags().StringToStringVar(&buffers, "buffers", map[string]string{}, "Buffers to validate") // --buffers=a=so,c=si,e=ed
	return command
}
