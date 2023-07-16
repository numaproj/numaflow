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

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/daemon/server"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/spf13/cobra"
)

func NewSideInputManagerCommand() *cobra.Command {
	var (
		isbSvcType string
	)
	command := &cobra.Command{
		Use:   "side-input-manager",
		Short: "Start a Side Inputa Manager",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := logging.NewLogger().Named("side-input-manager")

			pl, err := decodePipeline()
			if err != nil {
				return fmt.Errorf("failed to decode the pipeline spec: %v", err)
			}

			ctx := logging.WithLogger(context.Background(), logger)
			server := server.NewDaemonServer(pl, v1alpha1.ISBSvcType(isbSvcType))
			return server.Run(ctx)
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "jetstream", "ISB Service type, e.g. jetstream")
	return command
}
