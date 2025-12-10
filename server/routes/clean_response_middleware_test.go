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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestCleanResponseMiddleware(t *testing.T) {
	// Set Gin to test mode
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		handlerFunc    gin.HandlerFunc
		expectedBody   string
		expectedStatus int
	}{
		{
			name: "removes managedFields from simple JSON response",
			handlerFunc: func(c *gin.Context) {
				response := map[string]interface{}{
					"name":          "test-pipeline",
					"managedFields": "should be removed",
					"namespace":     "default",
				}
				c.JSON(http.StatusOK, response)
			},
			expectedBody:   `{"name":"test-pipeline","namespace":"default"}`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "removes managedFields from nested JSON response",
			handlerFunc: func(c *gin.Context) {
				response := map[string]interface{}{
					"kind": "Pipeline",
					"metadata": map[string]interface{}{
						"name":          "my-pipeline",
						"managedFields": "should be removed",
						"namespace":     "default",
					},
					"spec": map[string]interface{}{
						"vertices": []interface{}{
							map[string]interface{}{
								"name":          "input",
								"managedFields": "should be removed",
							},
						},
					},
				}
				c.JSON(http.StatusOK, response)
			},
			expectedBody:   `{"kind":"Pipeline","metadata":{"name":"my-pipeline","namespace":"default"},"spec":{"vertices":[{"name":"input"}]}}`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "handles response without managedFields",
			handlerFunc: func(c *gin.Context) {
				response := map[string]interface{}{
					"name":      "test-pipeline",
					"namespace": "default",
					"status":    "running",
				}
				c.JSON(http.StatusOK, response)
			},
			expectedBody:   `{"name":"test-pipeline","namespace":"default","status":"running"}`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "handles empty JSON response",
			handlerFunc: func(c *gin.Context) {
				c.JSON(http.StatusOK, map[string]interface{}{})
			},
			expectedBody:   `{}`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "handles JSON array response (leaves unchanged - middleware only processes objects)",
			handlerFunc: func(c *gin.Context) {
				response := []interface{}{
					map[string]interface{}{
						"name":          "pipeline1",
						"managedFields": "remove",
					},
					map[string]interface{}{
						"name":          "pipeline2",
						"managedFields": "remove",
					},
				}
				c.JSON(http.StatusOK, response)
			},
			// Note: The middleware only processes JSON objects (maps), not arrays at root level
			// So the array response is returned as-is without cleaning
			expectedBody:   `[{"managedFields":"remove","name":"pipeline1"},{"managedFields":"remove","name":"pipeline2"}]`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "handles non-JSON response (plain text)",
			handlerFunc: func(c *gin.Context) {
				c.String(http.StatusOK, "plain text response")
			},
			expectedBody:   `plain text response`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "skips processing for text/html content type",
			handlerFunc: func(c *gin.Context) {
				c.Header("Content-Type", "text/html")
				c.String(http.StatusOK, "<html><body>HTML content</body></html>")
			},
			expectedBody:   `<html><body>HTML content</body></html>`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "skips processing for text/xml content type",
			handlerFunc: func(c *gin.Context) {
				c.Header("Content-Type", "text/xml")
				c.String(http.StatusOK, "<xml><data>test</data></xml>")
			},
			expectedBody:   `<xml><data>test</data></xml>`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "processes application/json content type",
			handlerFunc: func(c *gin.Context) {
				c.Header("Content-Type", "application/json")
				response := map[string]interface{}{
					"name":          "test",
					"managedFields": "should be removed",
				}
				c.JSON(http.StatusOK, response)
			},
			expectedBody:   `{"name":"test"}`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "processes application/json; charset=utf-8 content type",
			handlerFunc: func(c *gin.Context) {
				response := map[string]interface{}{
					"name":          "test",
					"managedFields": "should be removed",
				}
				c.JSON(http.StatusOK, response)
			},
			expectedBody:   `{"name":"test"}`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "handles error status codes",
			handlerFunc: func(c *gin.Context) {
				response := map[string]interface{}{
					"error":         "something went wrong",
					"managedFields": "should still be removed",
				}
				c.JSON(http.StatusInternalServerError, response)
			},
			expectedBody:   `{"error":"something went wrong"}`,
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name: "handles 404 status",
			handlerFunc: func(c *gin.Context) {
				response := map[string]interface{}{
					"error":         "not found",
					"managedFields": "remove",
				}
				c.JSON(http.StatusNotFound, response)
			},
			expectedBody:   `{"error":"not found"}`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name: "handles deeply nested structure with multiple managedFields",
			handlerFunc: func(c *gin.Context) {
				response := map[string]interface{}{
					"apiVersion": "v1",
					"items": []interface{}{
						map[string]interface{}{
							"metadata": map[string]interface{}{
								"name":          "item1",
								"managedFields": "remove1",
								"annotations": map[string]interface{}{
									"key":           "value",
									"managedFields": "remove2",
								},
							},
						},
						map[string]interface{}{
							"metadata": map[string]interface{}{
								"name":          "item2",
								"managedFields": "remove3",
							},
						},
					},
					"managedFields": "remove4",
				}
				c.JSON(http.StatusOK, response)
			},
			expectedBody:   `{"apiVersion":"v1","items":[{"metadata":{"annotations":{"key":"value"},"name":"item1"}},{"metadata":{"name":"item2"}}]}`,
			expectedStatus: http.StatusOK,
		},
		{
			name: "preserves other fields and data types",
			handlerFunc: func(c *gin.Context) {
				response := map[string]interface{}{
					"name":          "test",
					"count":         42,
					"active":        true,
					"value":         3.14,
					"nullable":      nil,
					"managedFields": "remove",
					"tags":          []string{"tag1", "tag2"},
				}
				c.JSON(http.StatusOK, response)
			},
			expectedBody:   `{"active":true,"count":42,"name":"test","nullable":null,"tags":["tag1","tag2"],"value":3.14}`,
			expectedStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new gin router with the middleware
			router := gin.New()
			router.Use(cleanResponseMiddleware())
			router.GET("/test", tt.handlerFunc)

			// Create a test request
			req, err := http.NewRequest(http.MethodGet, "/test", nil)
			assert.NoError(t, err)

			// Create a response recorder
			w := httptest.NewRecorder()

			// Serve the request
			router.ServeHTTP(w, req)

			// Assert status code
			assert.Equal(t, tt.expectedStatus, w.Code)

			// Assert response body
			// For JSON responses, we need to compare the actual JSON structure
			// not just the string, as field order might differ
			if tt.expectedStatus == http.StatusOK || w.Header().Get("Content-Type") == "application/json; charset=utf-8" {
				var expected, actual interface{}
				if json.Unmarshal([]byte(tt.expectedBody), &expected) == nil && json.Unmarshal(w.Body.Bytes(), &actual) == nil {
					assert.Equal(t, expected, actual)
				} else {
					// If not JSON, compare as strings
					assert.Equal(t, tt.expectedBody, w.Body.String())
				}
			} else {
				assert.Equal(t, tt.expectedBody, w.Body.String())
			}
		})
	}
}

func TestCleanResponseMiddleware_HandlerChain(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("middleware allows handler chain to continue", func(t *testing.T) {
		router := gin.New()

		var middlewareCalled bool
		var handlerCalled bool

		// Add clean response middleware
		router.Use(cleanResponseMiddleware())

		// Add another middleware to verify chain continues
		router.Use(func(c *gin.Context) {
			middlewareCalled = true
			c.Next()
		})

		// Add handler
		router.GET("/test", func(c *gin.Context) {
			handlerCalled = true
			c.JSON(http.StatusOK, map[string]interface{}{
				"status":        "ok",
				"managedFields": "remove",
			})
		})

		req, _ := http.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.True(t, middlewareCalled, "middleware should be called")
		assert.True(t, handlerCalled, "handler should be called")
		assert.Equal(t, http.StatusOK, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "ok", response["status"])
		assert.NotContains(t, response, "managedFields")
	})
}

func TestCleanResponseMiddleware_CustomResponseWriter(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("custom response writer captures response correctly", func(t *testing.T) {
		router := gin.New()
		router.Use(cleanResponseMiddleware())

		router.POST("/test", func(c *gin.Context) {
			// Test with multiple writes
			response := map[string]interface{}{
				"message":       "success",
				"managedFields": "should be removed",
				"data": map[string]interface{}{
					"id":            1,
					"managedFields": "also removed",
				},
			}
			c.JSON(http.StatusCreated, response)
		})

		req, _ := http.NewRequest(http.MethodPost, "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusCreated, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, "success", response["message"])
		assert.NotContains(t, response, "managedFields")

		data, ok := response["data"].(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, float64(1), data["id"])
		assert.NotContains(t, data, "managedFields")
	})
}

func TestCustomResponseWriter_Write(t *testing.T) {
	t.Run("custom response writer Write method", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)

		customWriter := &customResponseWriter{
			ResponseWriter: c.Writer,
			body:           &bytes.Buffer{},
		}

		data := []byte(`{"test":"data"}`)
		n, err := customWriter.Write(data)

		assert.NoError(t, err)
		assert.Equal(t, len(data), n)
		assert.Equal(t, data, customWriter.body.Bytes())
	})

	t.Run("custom response writer WriteString method", func(t *testing.T) {
		gin.SetMode(gin.TestMode)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)

		customWriter := &customResponseWriter{
			ResponseWriter: c.Writer,
			body:           &bytes.Buffer{},
		}

		testString := "test string"
		n, err := customWriter.WriteString(testString)

		assert.NoError(t, err)
		assert.Equal(t, len(testString), n)
		assert.Equal(t, testString, customWriter.body.String())
	})
}

func TestCleanResponseMiddleware_EdgeCases(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("handles empty response body", func(t *testing.T) {
		router := gin.New()
		router.Use(cleanResponseMiddleware())
		router.GET("/test", func(c *gin.Context) {
			c.Status(http.StatusNoContent)
		})

		req, _ := http.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusNoContent, w.Code)
		assert.Empty(t, w.Body.String())
	})

	t.Run("handles malformed JSON gracefully", func(t *testing.T) {
		router := gin.New()
		router.Use(cleanResponseMiddleware())
		router.GET("/test", func(c *gin.Context) {
			// Write malformed JSON directly
			c.Writer.WriteHeader(http.StatusOK)
			_, _ = c.Writer.Write([]byte(`{invalid json`))
		})

		req, _ := http.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		// Should return the original malformed JSON as-is
		assert.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, `{invalid json`, w.Body.String())
	})

	t.Run("handles large nested structures", func(t *testing.T) {
		router := gin.New()
		router.Use(cleanResponseMiddleware())
		router.GET("/test", func(c *gin.Context) {
			// Create a large nested structure
			items := make([]interface{}, 100)
			for i := 0; i < 100; i++ {
				items[i] = map[string]interface{}{
					"id":            i,
					"name":          "item" + string(rune(i)),
					"managedFields": "remove",
					"nested": map[string]interface{}{
						"managedFields": "remove",
						"value":         i * 2,
					},
				}
			}
			response := map[string]interface{}{
				"items":         items,
				"managedFields": "remove",
			}
			c.JSON(http.StatusOK, response)
		})

		req, _ := http.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		assert.Equal(t, http.StatusOK, w.Code)

		var response map[string]interface{}
		err := json.Unmarshal(w.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotContains(t, response, "managedFields")

		items, ok := response["items"].([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 100, len(items))

		// Check first and last items
		firstItem := items[0].(map[string]interface{})
		assert.NotContains(t, firstItem, "managedFields")
		assert.Contains(t, firstItem, "nested")

		nested := firstItem["nested"].(map[string]interface{})
		assert.NotContains(t, nested, "managedFields")
	})
}

func TestRemoveFieldsRecursively(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		fields   []string
		expected interface{}
	}{
		{
			name: "simple map with single field to remove",
			input: map[string]interface{}{
				"name":          "test",
				"managedFields": "should be removed",
				"value":         42,
			},
			fields: []string{"managedFields"},
			expected: map[string]interface{}{
				"name":  "test",
				"value": 42,
			},
		},
		{
			name: "nested map with field to remove",
			input: map[string]interface{}{
				"name": "test",
				"metadata": map[string]interface{}{
					"managedFields": "should be removed",
					"namespace":     "default",
				},
				"value": 42,
			},
			fields: []string{"managedFields"},
			expected: map[string]interface{}{
				"name": "test",
				"metadata": map[string]interface{}{
					"namespace": "default",
				},
				"value": 42,
			},
		},
		{
			name: "deeply nested map with field to remove at multiple levels",
			input: map[string]interface{}{
				"name":          "test",
				"managedFields": "level1",
				"nested": map[string]interface{}{
					"managedFields": "level2",
					"data":          "keep this",
					"deepNested": map[string]interface{}{
						"managedFields": "level3",
						"value":         100,
					},
				},
			},
			fields: []string{"managedFields"},
			expected: map[string]interface{}{
				"name": "test",
				"nested": map[string]interface{}{
					"data": "keep this",
					"deepNested": map[string]interface{}{
						"value": 100,
					},
				},
			},
		},
		{
			name: "array with maps containing field to remove",
			input: []interface{}{
				map[string]interface{}{
					"name":          "item1",
					"managedFields": "should be removed",
				},
				map[string]interface{}{
					"name":          "item2",
					"managedFields": "should be removed",
				},
			},
			fields: []string{"managedFields"},
			expected: []interface{}{
				map[string]interface{}{
					"name": "item1",
				},
				map[string]interface{}{
					"name": "item2",
				},
			},
		},
		{
			name: "map with array containing nested maps with field to remove",
			input: map[string]interface{}{
				"name": "test",
				"items": []interface{}{
					map[string]interface{}{
						"name":          "item1",
						"managedFields": "remove",
					},
					map[string]interface{}{
						"name":          "item2",
						"managedFields": "remove",
					},
				},
			},
			fields: []string{"managedFields"},
			expected: map[string]interface{}{
				"name": "test",
				"items": []interface{}{
					map[string]interface{}{
						"name": "item1",
					},
					map[string]interface{}{
						"name": "item2",
					},
				},
			},
		},
		{
			name: "multiple fields to remove",
			input: map[string]interface{}{
				"name":          "test",
				"managedFields": "remove1",
				"annotations":   "remove2",
				"value":         42,
			},
			fields: []string{"managedFields", "annotations"},
			expected: map[string]interface{}{
				"name":  "test",
				"value": 42,
			},
		},
		{
			name: "multiple fields to remove in nested structure",
			input: map[string]interface{}{
				"name":          "test",
				"managedFields": "remove1",
				"metadata": map[string]interface{}{
					"annotations":   "remove2",
					"managedFields": "remove3",
					"namespace":     "default",
				},
			},
			fields: []string{"managedFields", "annotations"},
			expected: map[string]interface{}{
				"name": "test",
				"metadata": map[string]interface{}{
					"namespace": "default",
				},
			},
		},
		{
			name:     "empty map",
			input:    map[string]interface{}{},
			fields:   []string{"managedFields"},
			expected: map[string]interface{}{},
		},
		{
			name:     "empty array",
			input:    []interface{}{},
			fields:   []string{"managedFields"},
			expected: []interface{}{},
		},
		{
			name: "map without the field to remove",
			input: map[string]interface{}{
				"name":  "test",
				"value": 42,
			},
			fields: []string{"managedFields"},
			expected: map[string]interface{}{
				"name":  "test",
				"value": 42,
			},
		},
		{
			name: "complex nested structure with arrays and maps",
			input: map[string]interface{}{
				"kind": "Pipeline",
				"metadata": map[string]interface{}{
					"name":          "my-pipeline",
					"managedFields": "should be removed",
				},
				"spec": map[string]interface{}{
					"vertices": []interface{}{
						map[string]interface{}{
							"name":          "input",
							"managedFields": "should be removed",
							"source": map[string]interface{}{
								"http":          map[string]interface{}{},
								"managedFields": "should be removed",
							},
						},
						map[string]interface{}{
							"name": "output",
							"sink": map[string]interface{}{
								"log":           map[string]interface{}{},
								"managedFields": "should be removed",
							},
						},
					},
				},
			},
			fields: []string{"managedFields"},
			expected: map[string]interface{}{
				"kind": "Pipeline",
				"metadata": map[string]interface{}{
					"name": "my-pipeline",
				},
				"spec": map[string]interface{}{
					"vertices": []interface{}{
						map[string]interface{}{
							"name": "input",
							"source": map[string]interface{}{
								"http": map[string]interface{}{},
							},
						},
						map[string]interface{}{
							"name": "output",
							"sink": map[string]interface{}{
								"log": map[string]interface{}{},
							},
						},
					},
				},
			},
		},
		{
			name: "primitive types in array should remain unchanged",
			input: []interface{}{
				"string",
				42,
				true,
				nil,
			},
			fields: []string{"managedFields"},
			expected: []interface{}{
				"string",
				42,
				true,
				nil,
			},
		},
		{
			name:     "primitive value should remain unchanged",
			input:    "string value",
			fields:   []string{"managedFields"},
			expected: "string value",
		},
		{
			name:     "nil value",
			input:    nil,
			fields:   []string{"managedFields"},
			expected: nil,
		},
		{
			name: "map with mixed primitive and complex values",
			input: map[string]interface{}{
				"string":        "value",
				"number":        42,
				"boolean":       true,
				"null":          nil,
				"managedFields": "remove",
				"nested": map[string]interface{}{
					"managedFields": "remove",
					"data":          "keep",
				},
			},
			fields: []string{"managedFields"},
			expected: map[string]interface{}{
				"string":  "value",
				"number":  42,
				"boolean": true,
				"null":    nil,
				"nested": map[string]interface{}{
					"data": "keep",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of input to avoid modifying test data
			var input interface{}
			switch v := tt.input.(type) {
			case map[string]interface{}:
				input = copyMap(v)
			case []interface{}:
				input = copySlice(v)
			default:
				input = tt.input
			}

			removeFieldsRecursively(input, tt.fields...)
			assert.Equal(t, tt.expected, input)
		})
	}
}

// Helper function to deep copy a map
func copyMap(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		switch val := v.(type) {
		case map[string]interface{}:
			result[k] = copyMap(val)
		case []interface{}:
			result[k] = copySlice(val)
		default:
			result[k] = val
		}
	}
	return result
}

// Helper function to deep copy a slice
func copySlice(s []interface{}) []interface{} {
	result := make([]interface{}, len(s))
	for i, v := range s {
		switch val := v.(type) {
		case map[string]interface{}:
			result[i] = copyMap(val)
		case []interface{}:
			result[i] = copySlice(val)
		default:
			result[i] = val
		}
	}
	return result
}
