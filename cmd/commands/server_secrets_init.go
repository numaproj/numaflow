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
	"fmt"

	"github.com/spf13/cobra"

	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
	secretinitcmd "github.com/numaproj/numaflow/server/cmd/serversecretinit"
)

func NewServerSecretsInitCommand() *cobra.Command {
	var disableAuth bool

	command := &cobra.Command{
		Use:   "server-secrets-init",
		Short: "Initialize numaflow-server-secrets with admin password and jwt secret key if needed",
		RunE: func(cmd *cobra.Command, args []string) error {
			if disableAuth { // Skip when auth is disabled
				return nil
			}
			if err := secretinitcmd.Start(); err != nil {
				return fmt.Errorf("failed to start server secrets init service: %w", err)
			}
			return nil
		},
	}
	command.Flags().BoolVar(&disableAuth, "disable-auth", sharedutil.LookupEnvBoolOr("NUMAFLOW_SERVER_DISABLE_AUTH", false), "Whether to disable authentication and authorization, defaults to false.")
	return command
}
