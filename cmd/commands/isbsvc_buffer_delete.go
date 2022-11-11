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

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func NewISBSvcBufferDeleteCommand() *cobra.Command {
	var (
		isbSvcType string
		buffers    map[string]string
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
				isbsClient = isbsvc.NewISBRedisSvc(redisclient.NewInClusterRedisClient())
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
			bfs := []v1alpha1.Buffer{}
			for k, v := range buffers {
				bfs = append(bfs, v1alpha1.Buffer{Name: k, Type: v1alpha1.BufferType(v)})
			}
			if err = isbsClient.DeleteBuffers(ctx, bfs); err != nil {
				logger.Errorw("Failed buffer deletion.", zap.Error(err))
				return err
			}
			logger.Info("Deleted Buffers successfully")
			return nil
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "jetstream", "ISB Service type, e.g. jetstream")
	command.Flags().StringToStringVar(&buffers, "buffers", map[string]string{}, "Buffers to delete") // --buffers=a=so,c=si,e=ed
	return command
}
