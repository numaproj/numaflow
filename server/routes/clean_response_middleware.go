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

// customResponseWriter wraps gin.ResponseWriter to buffer the response body for
// post-processing. A handler that calls Flush() is treated as streaming: the
// writer drains any buffered bytes, flips into pass-through mode, and forwards
// every subsequent Write directly to the underlying ResponseWriter. Handlers
// that never call Flush() remain fully buffered so the middleware can rewrite
// the response (e.g. stripping managedFields).
//
// Note: a handler that streams without ever calling Flush() would still be
// buffered. All current streaming endpoints (pod logs follow, c.Stream-based
// handlers) call Flush(), so this is sufficient in practice. Hijacked
// connections (e.g. websockets) bypass Write entirely and are unaffected.
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
		return w.ResponseWriter.Write([]byte(s))
	}
	return w.body.WriteString(s)
}

// Flush is the streaming signal. On the first call we drain any bytes that
// were buffered before the handler decided to stream, flip into pass-through
// mode, and then forward to the underlying writer's Flush.
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
