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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"github.com/numaproj/numaflow"
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/pkg/sideinputs/manager"
)

func NewSideInputsManagerCommand() *cobra.Command {
	var (
		isbSvcType      string
		sideInputsStore string
	)
	command := &cobra.Command{
		Use:   "side-inputs-manager",
		Short: "Start a Side Inputs Manager",
		RunE: func(cmd *cobra.Command, args []string) error {
			encodedSiceInputSpec, defined := os.LookupEnv(dfv1.EnvSideInputObject)
			if !defined {
				return fmt.Errorf("environment %q is not defined", dfv1.EnvSideInputObject)
			}
			decodeSideInputBytes, err := base64.StdEncoding.DecodeString(encodedSiceInputSpec)
			if err != nil {
				return fmt.Errorf("failed to decode encoded SideInput object, error: %w", err)
			}
			sideInput := &dfv1.SideInput{}
			if err = json.Unmarshal(decodeSideInputBytes, sideInput); err != nil {
				return fmt.Errorf("failed to unmarshal SideInput object, error: %w", err)
			}

			pipelineName, defined := os.LookupEnv(dfv1.EnvPipelineName)
			if !defined {
				return fmt.Errorf("environment %q is not defined", dfv1.EnvPipelineName)
			}

			logger := logging.NewLogger().Named("side-inputs-manager").With("pipeline", pipelineName)
			logger.Infow("Starting side inputs manager", "version", numaflow.GetVersion())

			ctx := logging.WithLogger(signals.SetupSignalHandler(), logger)
			sideInputManager := manager.NewSideInputsManager(dfv1.ISBSvcType(isbSvcType), pipelineName, sideInputsStore, sideInput)
			return sideInputManager.Start(ctx)
		},
	}
	command.Flags().StringVar(&isbSvcType, "isbsvc-type", "jetstream", "ISB Service type, e.g. jetstream")
	command.Flags().StringVar(&sideInputsStore, "side-inputs-store", "", "Name of the side inputs store")
	return command
}
