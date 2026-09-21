/*
Copyright 2026 The Numaproj Authors.

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
	"fmt"

	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/server/apis/v2/generated"
)

// RequestIDHeader carries a caller-provided trace identifier into a Problem response.
const RequestIDHeader = "X-Request-ID"

// WriteProblem writes an API v2 error using the OpenAPI Problem response shape
// and stops later Gin handlers from writing another response.
func WriteProblem(c *gin.Context, status int, code, title, detail string, violations []generated.Violation) {
	problem := generated.Problem{
		Type:     fmt.Sprintf("/api/v2/problems/%s", code),
		Title:    title,
		Status:   status,
		Code:     code,
		Detail:   detail,
		Instance: c.Request.URL.Path,
	}
	if traceID := c.GetHeader(RequestIDHeader); traceID != "" {
		problem.TraceId = &traceID
	}
	if len(violations) > 0 {
		problem.Violations = &violations
	}
	c.Header("Content-Type", "application/problem+json")
	c.Abort()
	c.JSON(status, problem)
}
