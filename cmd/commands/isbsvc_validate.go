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
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

func NewISBSvcValidateCommand() *cobra.Command {

	var (
		isbSvcType string
		buffers    []string
		buckets    []string
	)

	command := &cobra.Command{
		Use:   "isbsvc-validate",
		Short: "Validate ISB Service buffers and buckets",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.NewLogger().Named("isbsvc-validate")
			pipelineName, existing := os.LookupEnv(v1alpha1.EnvPipelineName)
			if !existing {
				return fmt.Errorf("environment variable %q not existing", v1alpha1.EnvPipelineName)
			}
			var isbsClient isbsvc.ISBService
			var err error
			ctx := logging.WithLogger(context.Background(), logger)
			switch v1alpha1.ISBSvcType(isbSvcType) {
			case v1alpha1.ISBSvcTypeRedis:
				isbsClient = isbsvc.NewISBRedisSvc(redisclient.NewInClusterRedisClient())
			case v1alpha1.ISBSvcTypeJetStream:
				isbsClient, err = isbsvc.NewISBJetStreamSvc(pipelineName)
				if err != nil {
					logger.Errorw("Failed to get an ISB Service client.", zap.Error(err))
					return err
				}
			default:
				cmd.HelpFunc()(cmd, args)
				return fmt.Errorf("unsupported isb service type")
			}
			_ = wait.ExponentialBackoffWithContext(ctx, sharedutil.DefaultRetryBackoff, func() (bool, error) {
				if err = isbsClient.ValidateBuffersAndBuckets(ctx, buffers, buckets); err != nil {
					logger.Infow("Buffers and buckets might have not been created yet, will retry if the limit is not reached", zap.Error(err))
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				logger.Errorw("Failed on buffer and bucket validation after retrying.", zap.Error(err))
				return err
			}
			logger.Info("Validate buffers and buckets successfully")
			return nil
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "", "ISB Service type, e.g. jetstream")
	command.Flags().StringSliceVar(&buffers, "buffers", []string{}, "Buffers to validate") // --buffers=a,b, --buffers=c
	command.Flags().StringSliceVar(&buckets, "buckets", []string{}, "Buckets to validate") // --buckets=xxa,xxb --buckets=xxc
	return command
}
