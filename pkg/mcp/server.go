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
	"crypto/tls"
	"fmt"
	"net/http"

	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/numaproj/numaflow"
	sharedtls "github.com/numaproj/numaflow/pkg/shared/tls"
)

// HTTPOptions configures the streamable-HTTP transport.
type HTTPOptions struct {
	// Port to listen on (e.g. 8443).
	Port int
	// Insecure disables TLS. For development only — logs a warning on startup.
	Insecure bool
	// CertFile / KeyFile point to PEM-encoded TLS material. When both are empty
	// and Insecure is false a self-signed certificate is generated automatically.
	CertFile string
	KeyFile  string
}

// NewServer builds an MCP server that exposes the read-only Numaflow tools
// provided by the given registry.
func NewServer(reg ToolRegistry) *mcpserver.MCPServer {
	s := mcpserver.NewMCPServer(
		"Numaflow MCP Server",
		numaflow.GetVersion().Version,
		mcpserver.WithToolCapabilities(false),
	)
	for _, td := range reg.Tools() {
		s.AddTool(td.Tool, td.Handler)
	}
	return s
}

// ServeStdio builds the MCP server from the registry and serves it over stdio
// until the client disconnects or the process is signalled.
func ServeStdio(reg ToolRegistry) error {
	return mcpserver.ServeStdio(NewServer(reg))
}

// ServeHTTP builds the MCP server and serves it over streamable HTTP at /mcp
// on the configured port. When opts.Insecure is false a TLS listener is used;
// if no cert/key files are provided a self-signed certificate is generated.
func ServeHTTP(reg ToolRegistry, opts HTTPOptions) error {
	addr := fmt.Sprintf(":%d", opts.Port)

	if opts.Insecure {
		fmt.Printf("WARNING: Numaflow MCP server starting without TLS on %s — do not expose outside a trusted network\n", addr)
		s := mcpserver.NewStreamableHTTPServer(
			NewServer(reg),
			mcpserver.WithEndpointPath("/mcp"),
		)
		return s.Start(addr)
	}

	tlsCfg, err := buildTLSConfig(opts.CertFile, opts.KeyFile)
	if err != nil {
		return fmt.Errorf("failed to build TLS config: %w", err)
	}

	httpSrv := &http.Server{
		Addr:      addr,
		TLSConfig: tlsCfg,
	}
	s := mcpserver.NewStreamableHTTPServer(
		NewServer(reg),
		mcpserver.WithEndpointPath("/mcp"),
		mcpserver.WithStreamableHTTPServer(httpSrv),
	)
	return s.Start(addr)
}

func buildTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	var cert *tls.Certificate
	if certFile != "" && keyFile != "" {
		c, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS key pair: %w", err)
		}
		cert = &c
	} else {
		c, err := sharedtls.GenerateX509KeyPair()
		if err != nil {
			return nil, fmt.Errorf("failed to generate self-signed certificate: %w", err)
		}
		cert = c
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{*cert},
	}, nil
}
