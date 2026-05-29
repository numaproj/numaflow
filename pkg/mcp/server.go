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

package mcp

import (
	"github.com/mark3labs/mcp-go/server"

	"github.com/numaproj/numaflow"
)

// NewServer builds an MCP server that exposes the read-only Numaflow tools
// provided by the given registry.
func NewServer(reg ToolRegistry) *server.MCPServer {
	s := server.NewMCPServer(
		"Numaflow MCP Server",
		numaflow.GetVersion().Version,
		server.WithToolCapabilities(false),
	)
	for _, td := range reg.Tools() {
		s.AddTool(td.Tool, td.Handler)
	}
	return s
}

// ServeStdio builds the MCP server from the registry and serves it over stdio
// until the client disconnects or the process is signalled.
func ServeStdio(reg ToolRegistry) error {
	return server.ServeStdio(NewServer(reg))
}
