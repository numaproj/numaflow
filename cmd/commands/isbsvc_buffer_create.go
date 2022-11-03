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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
	redisclient "github.com/numaproj/numaflow/pkg/shared/clients/redis"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func NewISBSvcBufferCreateCommand() *cobra.Command {

	var (
		isbSvcType string
		buffers    map[string]string
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
				isbsClient = isbsvc.NewISBRedisSvc(redisclient.NewInClusterRedisClient())
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

			bfs := []v1alpha1.Buffer{}
			for k, v := range buffers {
				bfs = append(bfs, v1alpha1.Buffer{Name: k, Type: v1alpha1.BufferType(v)})
			}
			if err = isbsClient.CreateBuffers(ctx, bfs, opts...); err != nil {
				logger.Errorw("Failed to create buffers.", zap.Error(err))
				return err
			}
			logger.Info("Created buffers successfully")
			return nil
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "jetstream", "ISB Service type, e.g. jetstream")
	command.Flags().StringToStringVar(&buffers, "buffers", map[string]string{}, "Buffers to create") // --buffers=a=so,c=si,e=ed
	return command
}
