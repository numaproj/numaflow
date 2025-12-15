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

package routes

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/gin-gonic/gin"
)

var (
	// "managedFields" is too annoying
	cleanupFields = []string{"managedFields"}
)

// customResponseWriter wraps gin.ResponseWriter to capture the response body.
type customResponseWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w customResponseWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}

func (w customResponseWriter) WriteString(s string) (int, error) {
	return w.body.WriteString(s)
}

// cleanResponseMiddleware is a Gin middleware to clean up data in the response body.
func cleanResponseMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Replace the original response writer with our custom one
		w := &customResponseWriter{body: bytes.NewBufferString(""), ResponseWriter: c.Writer}
		c.Writer = w

		c.Next()

		// At this point, the original handler has written to our customResponseWriter's body buffer.
		// We can now access and modify the response data.
		originalBody := w.body.Bytes()

		// Only process JSON responses
		contentType := w.ResponseWriter.Header().Get("Content-Type")
		if !strings.Contains(contentType, "application/json") {
			_, _ = w.ResponseWriter.Write(originalBody)
			return
		}

		var data map[string]interface{}
		if err := json.Unmarshal(originalBody, &data); err == nil {
			removeFieldsRecursively(data, cleanupFields...)

			cleanedBody, err := json.Marshal(data)
			if err == nil {
				_, _ = w.ResponseWriter.Write(cleanedBody)
				return
			}
		}
		_, _ = w.ResponseWriter.Write(originalBody)
	}
}

// removeFieldRecursively removes all occurrences of the given field from a (possibly nested) JSON structure represented as map[string]interface{} or []interface{}.
func removeFieldsRecursively(data interface{}, fields ...string) {
	switch val := data.(type) {
	case map[string]interface{}:
		for _, f := range fields {
			delete(val, f)
		}
		// Recursively check each value in the map
		for _, v := range val {
			removeFieldsRecursively(v, fields...)
		}
	case []interface{}:
		// Recursively check each item in the array
		for _, item := range val {
			removeFieldsRecursively(item, fields...)
		}
	}
}
