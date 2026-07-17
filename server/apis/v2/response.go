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

package v2

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// APIResponse is the success envelope for v2 endpoints.
type APIResponse struct {
	Data any `json:"data"`
}

// ErrorBody is the structured error payload for v2 endpoints.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// APIError is the error envelope for v2 endpoints.
type APIError struct {
	Error ErrorBody `json:"error"`
}

// WriteOK writes a successful JSON response with HTTP 200.
func WriteOK(c *gin.Context, data any) {
	c.JSON(http.StatusOK, APIResponse{Data: data})
}

// WriteError writes a structured JSON error with the given HTTP status code.
func WriteError(c *gin.Context, status int, code, message string) {
	c.JSON(status, APIError{
		Error: ErrorBody{
			Code:    code,
			Message: message,
		},
	})
}
