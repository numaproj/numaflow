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
	"os"

	"github.com/spf13/cobra"

	"github.com/numaproj/numaflow/pkg/mcp"
	sharedutil "github.com/numaproj/numaflow/pkg/shared/util"
)

func NewMCPServerCommand() *cobra.Command {
	var (
		kubeconfig string
		namespace  string
	)

	command := &cobra.Command{
		Use:   "mcp-server",
		Short: "Start the Numaflow MCP server (read-only) over stdio",
		Long: `Start a read-only Model Context Protocol (MCP) server for Numaflow over stdio.

It exposes read-only tools (list/get Pipelines, MonoVertices and
InterStepBufferServices) backed by the Kubernetes API, using the ambient
kubeconfig (KUBECONFIG or ~/.kube/config) or the in-cluster config. It performs
no create, update, delete or patch operations.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if kubeconfig != "" {
				if err := os.Setenv("KUBECONFIG", kubeconfig); err != nil {
					return err
				}
			}
			numaflowClient, err := mcp.NewNumaflowClient()
			if err != nil {
				return err
			}
			return mcp.ServeStdio(mcp.NewRegistry(numaflowClient, namespace))
		},
	}
	command.Flags().StringVar(&kubeconfig, "kubeconfig", "", "Path to the kubeconfig file (defaults to $KUBECONFIG or ~/.kube/config; falls back to in-cluster config).")
	command.Flags().StringVarP(&namespace, "namespace", "n", sharedutil.LookupEnvStringOr("NAMESPACE", ""), "Default namespace used when a tool call omits one. Empty means all namespaces for list operations.")
	return command
}
