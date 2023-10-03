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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sideinputs/initializer"
	"github.com/spf13/cobra"
)

func NewSideInputsInitCommand() *cobra.Command {
	var (
		isbSvcType      string
		sideInputsStore string
		sideInputs      []string
	)
	command := &cobra.Command{
		Use:   "side-inputs-init",
		Short: "Start the Side Inputs init service",
		RunE: func(cmd *cobra.Command, args []string) error {

			pipelineName, defined := os.LookupEnv(dfv1.EnvPipelineName)
			if !defined {
				return fmt.Errorf("environment %q is not defined", dfv1.EnvPipelineName)
			}

			if len(sideInputs) == 0 {
				return fmt.Errorf("no side inputs are defined for this vertex")
			}
			logger := logging.NewLogger().Named("side-inputs-init").With("pipeline", pipelineName)
			ctx := logging.WithLogger(context.Background(), logger)
			sideInputsInitializer := initializer.NewSideInputsInitializer(dfv1.ISBSvcType(isbSvcType), pipelineName, sideInputsStore, sideInputs)
			return sideInputsInitializer.Run(ctx)
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "jetstream", "ISB Service type, e.g. jetstream")
	command.Flags().StringVar(&sideInputsStore, "side-inputs-store", "", "Name of the side inputs store")
	command.Flags().StringSliceVar(&sideInputs, "side-inputs", []string{}, "Side Input names") // --side-inputs=si1,si2 --side-inputs=si3
	return command
}
