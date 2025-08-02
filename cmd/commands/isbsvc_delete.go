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
	jsclient "github.com/numaproj/numaflow/pkg/shared/clients/nats"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func NewISBSvcDeleteCommand() *cobra.Command {
	var (
		isbSvcType         string
		buffers            []string
		buckets            []string
		sideInputsStore    string
		servingSourceStore string
	)

	command := &cobra.Command{
		Use:   "isbsvc-delete",
		Short: "Delete ISB Service buffers, buckets and side inputs store",
		RunE: func(cmd *cobra.Command, args []string) error {
			pipelineName, defined := os.LookupEnv(v1alpha1.EnvPipelineName)
			if !defined {
				return fmt.Errorf("required environment variable '%s' not defined", v1alpha1.EnvPipelineName)
			}
			logger := logging.NewLogger().Named("isbsvc-delete").With("pipeline", pipelineName)
			var isbsClient isbsvc.ISBService
			var err error
			ctx := logging.WithLogger(context.Background(), logger)
			switch v1alpha1.ISBSvcType(isbSvcType) {
			case v1alpha1.ISBSvcTypeJetStream:
				client, err := jsclient.NewNATSClient(ctx)
				if err != nil {
					return fmt.Errorf("failed to get an in-cluster nats connection, %w", err)
				}
				defer client.Close()

				isbsClient, err = isbsvc.NewISBJetStreamSvc(client)
				if err != nil {
					logger.Errorw("Failed to get a ISB Service client.", zap.Error(err))
					return err
				}
			default:
				cmd.HelpFunc()(cmd, args)
				return fmt.Errorf("unsupported isb service type %q", isbSvcType)
			}
			if err = isbsClient.DeleteBuffersAndBuckets(ctx, buffers, buckets, sideInputsStore, servingSourceStore); err != nil {
				logger.Errorw("Failed on buffers, buckets and side inputs store deletion.", zap.Error(err))
				return err
			}
			logger.Info("Deleted buffers, buckets and side inputs store successfully")
			return nil
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "", "ISB Service type, e.g. jetstream")
	command.Flags().StringSliceVar(&buffers, "buffers", []string{}, "Buffers to delete") // --buffers=a,b, --buffers=c
	command.Flags().StringSliceVar(&buckets, "buckets", []string{}, "Buckets to delete") // --buckets=xxa,xxb --buckets=xxc	return command
	command.Flags().StringVar(&sideInputsStore, "side-inputs-store", "", "Name of the side inputs store")
	command.Flags().StringVar(&servingSourceStore, "serving-store", "", "Serving source store to delete") // --serving-store=a
	return command
}
