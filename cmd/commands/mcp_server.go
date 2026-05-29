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
		kubeconfig     string
		namespace      string
		httpMode       bool
		port           int
		insecure       bool
		certFile       string
		keyFile        string
		daemonProtocol string
	)

	command := &cobra.Command{
		Use:   "mcp-server",
		Short: "Start the Numaflow MCP server (read-only)",
		Long: `Start a read-only Model Context Protocol (MCP) server for Numaflow.

Exposes read-only tools (list/get Pipelines, MonoVertices, ISB Services, daemon
runtime diagnostics, pod info, logs, and events) backed by the Kubernetes API.

By default the server runs over stdio, suitable for local use with Cursor,
Claude Code, or any MCP client that supports the stdio transport.

Use --http to run as a streamable-HTTP server for in-cluster deployment.

The server performs no create, update, delete or patch operations.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if kubeconfig != "" {
				if err := os.Setenv("KUBECONFIG", kubeconfig); err != nil {
					return err
				}
			}
			kubeClient, numaflowClient, err := mcp.NewClients()
			if err != nil {
				return err
			}
			reg := mcp.NewRegistry(kubeClient, numaflowClient, namespace, daemonProtocol)

			if httpMode {
				if port == 0 {
					if insecure {
						port = 8080
					} else {
						port = 8443
					}
				}
				return mcp.ServeHTTP(reg, mcp.HTTPOptions{
					Port:     port,
					Insecure: insecure,
					CertFile: certFile,
					KeyFile:  keyFile,
				})
			}
			return mcp.ServeStdio(reg)
		},
	}

	command.Flags().StringVar(&kubeconfig, "kubeconfig", "",
		"Path to the kubeconfig file (defaults to $KUBECONFIG or ~/.kube/config; falls back to in-cluster config).")
	command.Flags().StringVarP(&namespace, "namespace", "n",
		sharedutil.LookupEnvStringOr("NAMESPACE", ""),
		"Default namespace used when a tool call omits one. Empty means all namespaces for list operations.")
	command.Flags().BoolVar(&httpMode, "http",
		sharedutil.LookupEnvStringOr("NUMAFLOW_MCP_HTTP", "") == "true",
		"Run as a streamable-HTTP server instead of stdio. Suitable for in-cluster deployment.")
	command.Flags().IntVar(&port, "port", 0,
		"Port for the HTTP server (default 8443, or 8080 when --insecure). Only used with --http.")
	command.Flags().BoolVar(&insecure, "insecure",
		sharedutil.LookupEnvStringOr("NUMAFLOW_MCP_INSECURE", "") == "true",
		"Disable TLS for the HTTP server. For development only. Only used with --http.")
	command.Flags().StringVar(&certFile, "tls-cert", "",
		"Path to TLS certificate file. When omitted a self-signed cert is generated. Only used with --http.")
	command.Flags().StringVar(&keyFile, "tls-key", "",
		"Path to TLS key file. When omitted a self-signed cert is generated. Only used with --http.")
	command.Flags().StringVar(&daemonProtocol, "daemon-client-protocol",
		sharedutil.LookupEnvStringOr("NUMAFLOW_DAEMON_CLIENT_PROTOCOL", "grpc"),
		"Protocol for daemon client connections: 'grpc' (default) or 'http'.")

	return command
}
