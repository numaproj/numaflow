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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestWriteOK(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	WriteOK(c, map[string]string{"hello": "world"})

	assert.Equal(t, http.StatusOK, w.Code)
	var resp APIResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	data, ok := resp.Data.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "world", data["hello"])
}

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	WriteError(c, http.StatusNotFound, "NOT_FOUND", "pipeline not found")

	assert.Equal(t, http.StatusNotFound, w.Code)
	var resp APIError
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "NOT_FOUND", resp.Error.Code)
	assert.Equal(t, "pipeline not found", resp.Error.Message)
}

func TestSysInfo(t *testing.T) {
	sysInfo := map[string]any{
		"managedNamespace": "numaflow-system",
		"namespaced":       false,
		"version":          "test",
	}
	h := NewHandler(sysInfo)

	router := gin.New()
	router.GET("/sysinfo", h.SysInfo)

	w := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/sysinfo", nil)
	require.NoError(t, err)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var resp APIResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	data, ok := resp.Data.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "numaflow-system", data["managedNamespace"])
	assert.Equal(t, false, data["namespaced"])
	assert.Equal(t, "test", data["version"])
}
