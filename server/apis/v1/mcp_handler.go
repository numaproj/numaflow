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

package v1

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/gin-gonic/gin"
	"github.com/mark3labs/mcp-go/mcp"

	"github.com/numaproj/numaflow"
)

const (
	mcpProtocolVersion = "2025-06-18"
)

type mcpHandler struct {
	tools map[string]ToolDefinition
}

func NewMCPHandler(toolRegistry ToolRegistry) *mcpHandler {
	toolDefinitions := toolRegistry.RegisteredTools()
	tools := make(map[string]ToolDefinition)
	for _, td := range toolDefinitions {
		tools[td.Tool.GetName()] = td
	}
	return &mcpHandler{
		tools: tools,
	}
}

func (s *mcpHandler) listTools() []mcp.Tool {
	result := make([]mcp.Tool, 0)
	for _, v := range s.tools {
		result = append(result, v.Tool)
	}
	slices.SortFunc(result, func(a, b mcp.Tool) int {
		if a.Name < b.Name {
			return -1
		} else if a.Name > b.Name {
			return 1
		}
		return 0 // Sort by name alphabetically
	})
	return result
}

func (s *mcpHandler) HandleGet(c *gin.Context) {
	// MCP server discovery endpoint
	c.JSON(http.StatusOK, buildMCPServerInfo())
}

// Handles POST full MCP JSON-RPC protocol
func (s *mcpHandler) HandlePost(c *gin.Context) {
	var jsonRPCRequest struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      any             `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params,omitempty"`
	}

	if err := c.ShouldBindJSON(&jsonRPCRequest); err != nil {
		c.JSON(http.StatusBadRequest, jsonRPCResponseError(nil, -32700, "Parse error", err.Error()))
		return
	}

	switch jsonRPCRequest.Method {
	case "initialize":
		// Handle MCP initialization
		c.JSON(http.StatusOK, jsonRPCResponseResult(jsonRPCRequest.ID, buildMCPServerInfo()))

	case "notifications/initialized":
		// Handle initialized notification (no response needed for notifications)
		c.Status(http.StatusOK)
		return

	case "tools/list":
		tools := s.listTools()
		c.JSON(http.StatusOK, jsonRPCResponseResult(jsonRPCRequest.ID, gin.H{
			"tools": tools,
		}))

	case "tools/call":
		var callRequest mcp.CallToolRequest
		if err := json.Unmarshal(jsonRPCRequest.Params, &callRequest.Params); err != nil {
			c.JSON(http.StatusBadRequest, jsonRPCResponseError(jsonRPCRequest.ID, -32602, "Invalid params", err.Error()))
			return
		}

		toolDef, exists := s.tools[callRequest.Params.Name]
		if !exists {
			c.JSON(http.StatusOK, jsonRPCResponseError(jsonRPCRequest.ID, -32601, "Method not found", "Tool not found: "+callRequest.Params.Name))
			return
		}

		ctx := c.Request.Context()
		result, err := toolDef.Handler(ctx, callRequest)
		if err != nil {
			c.JSON(http.StatusOK, jsonRPCResponseError(jsonRPCRequest.ID, -32603, "Internal error", err.Error()))
			return
		}

		c.JSON(http.StatusOK, jsonRPCResponseResult(jsonRPCRequest.ID, result))

	default:
		c.JSON(http.StatusOK, jsonRPCResponseError(jsonRPCRequest.ID, -32601, "Method not found", "Unknown method: "+jsonRPCRequest.Method))
	}
}

func jsonRPCResponseError(ID any, errCode int, message string, data string) gin.H {
	return gin.H{
		"jsonrpc": "2.0",
		"id":      ID,
		"error": gin.H{
			"code":    errCode,
			"message": message,
			"data":    data,
		},
	}
}

func jsonRPCResponseResult(ID any, result any) gin.H {
	return gin.H{
		"jsonrpc": "2.0",
		"id":      ID,
		"result":  result,
	}
}

func buildMCPServerInfo() gin.H {
	return gin.H{
		"protocolVersion": mcpProtocolVersion,
		"capabilities": gin.H{
			"tools": gin.H{},
		},
		"serverInfo": gin.H{
			"name":    "Numaflow MCP Server",
			"version": numaflow.GetVersion().String(),
		},
	}
}
