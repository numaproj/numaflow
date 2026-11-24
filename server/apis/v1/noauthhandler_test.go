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
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogoutWithoutLoginCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/auth/v1/logout", nil)

	h, err := NewNoAuthHandler(nil, nil)
	require.NoError(t, err)
	h.Logout(c)

	assert.Equal(t, http.StatusBadRequest, recorder.Code)

	// The body must be a single JSON response that carries the error.
	var resp NumaflowAPIResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &resp))
	require.NotNil(t, resp.ErrMsg)
	assert.Contains(t, *resp.ErrMsg, "Failed to retrieve login type cookie")
}
