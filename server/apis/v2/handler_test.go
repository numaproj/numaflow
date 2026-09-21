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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/numaproj/numaflow/server/apis/v2/generated"
	"github.com/numaproj/numaflow/server/application/podview"
)

func TestGetCapabilities(t *testing.T) {
	service := &fakePodViewService{capabilities: podview.Capabilities{
		APIVersion: "v2",
		PodView: podview.Capability{
			Mode:                 podview.ModeOptIn,
			Eligible:             true,
			DefaultExperience:    podview.ExperienceClassic,
			AllowClassicFallback: true,
		},
		Operations: []string{"getCapabilities"},
		Limits: podview.Limits{
			DefaultPageSize:     50,
			MaximumPageSize:     200,
			MaximumLogLines:     1000,
			MaximumMetricPoints: 2000,
		},
	}}
	router := testRouter(t, service)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/capabilities", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	var response generated.Capabilities
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Equal(t, "v2", response.ApiVersion)
	assert.Equal(t, generated.OptIn, response.PodView.Mode)
	assert.Equal(t, []string{"getCapabilities"}, response.Operations)
}

func TestNewHandlerRejectsNilService(t *testing.T) {
	_, err := NewHandler(nil)
	require.Error(t, err)
}

func testRouter(t *testing.T, service PodViewService) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	handler, err := NewHandler(service)
	require.NoError(t, err)
	router := gin.New()
	generated.RegisterHandlersWithOptions(router, handler, generated.GinServerOptions{
		ErrorHandler: func(c *gin.Context, err error, _ int) {
			WriteProblem(c, http.StatusUnprocessableEntity, "validation_failed", "Request validation failed", err.Error(), nil)
		},
	})
	return router
}

type fakePodViewService struct {
	capabilities podview.Capabilities
}

func (f *fakePodViewService) GetCapabilities() podview.Capabilities {
	return f.capabilities
}
