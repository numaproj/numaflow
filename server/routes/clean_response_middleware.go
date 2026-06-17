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
	// Fields stripped from buffered JSON responses.
	cleanupFields = []string{"managedFields"}
)

// customResponseWriter buffers non-streaming responses so the middleware can
// rewrite them. Calling Flush marks the response as streaming: buffered bytes
// are drained and subsequent writes pass through directly.
type customResponseWriter struct {
	gin.ResponseWriter
	body      *bytes.Buffer
	streaming bool
}

func (w *customResponseWriter) Write(b []byte) (int, error) {
	if w.streaming {
		return w.ResponseWriter.Write(b)
	}
	return w.body.Write(b)
}

func (w *customResponseWriter) WriteString(s string) (int, error) {
	if w.streaming {
		return w.ResponseWriter.WriteString(s)
	}
	return w.body.WriteString(s)
}

// Flush switches to streaming mode and forwards the flush to the underlying writer.
func (w *customResponseWriter) Flush() {
	if !w.streaming {
		w.streaming = true
		if w.body.Len() > 0 {
			_, _ = w.ResponseWriter.Write(w.body.Bytes())
			w.body.Reset()
		}
	}
	w.ResponseWriter.Flush()
}

// cleanResponseMiddleware is a Gin middleware to clean up data in the response body.
func cleanResponseMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		w := &customResponseWriter{body: bytes.NewBufferString(""), ResponseWriter: c.Writer}
		c.Writer = w

		c.Next()

		// If the handler streamed (called Flush at any point), bytes have
		// already been sent to the client and no cleanup is possible.
		if w.streaming {
			return
		}

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

// removeFieldsRecursively removes fields from nested JSON objects and arrays.
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
