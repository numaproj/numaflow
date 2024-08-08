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
	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/mvtxdaemon/server"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

func NewMonoVtxDaemonServerCommand() *cobra.Command {

	command := &cobra.Command{
		Use:   "mvtx-daemon-server",
		Short: "Start the mono vertex daemon server",
		RunE: func(cmd *cobra.Command, args []string) error {
			monoVtx, err := decodeMonoVtx()
			if err != nil {
				return fmt.Errorf("failed to decode the mono vertex spec: %v", err)
			}
			logger := logging.NewLogger().Named("mvtx-daemon-server").With("mvtx", monoVtx.Name)
			logger.Infow("Starting mono vertex daemon server", "version", numaflow.GetVersion())
			ctx := logging.WithLogger(signals.SetupSignalHandler(), logger)
			server := server.NewDaemonServer(monoVtx)
			return server.Run(ctx)
		},
	}
	return command
}

func decodeMonoVtx() (*v1alpha1.MonoVertex, error) {
	encodedMonoVtxSpec, defined := os.LookupEnv(v1alpha1.EnvMonoVertexObject)
	if !defined {
		return nil, fmt.Errorf("environment %q is not defined", v1alpha1.EnvMonoVertexObject)
	}
	decodedMonoVtxBytes, err := base64.StdEncoding.DecodeString(encodedMonoVtxSpec)

	if err != nil {
		return nil, fmt.Errorf("failed to decode the encoded MonoVertex object, error: %w", err)
	}
	monoVtx := &v1alpha1.MonoVertex{}
	if err = json.Unmarshal(decodedMonoVtxBytes, monoVtx); err != nil {
		return nil, fmt.Errorf("failed to unmarshal the MonoVertex object, error: %w", err)
	}
	return monoVtx, nil
}
