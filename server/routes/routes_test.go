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
	"net/http"
	"net/http/httptest"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/manager/signals"

	"github.com/gin-gonic/gin"
	"github.com/numaproj/numaflow/pkg/shared/logging"
	"github.com/numaproj/numaflow/server/authz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoutes(t *testing.T) {
	// skipping this test for the time being
	t.Skip()
	log := logging.NewLogger().Named("server")
	router := gin.Default()
	managedNamespace := "numaflow-system"
	namespaced := false
	sysInfo := SystemInfo{
		ManagedNamespace: managedNamespace,
		Namespaced:       namespaced,
		IsReadOnly:       false,
	}

	authInfo := AuthInfo{
		DisableAuth:   false,
		DexServerAddr: "test-dex-server-addr",
	}

	authRouteMap := authz.RouteMap{}
	Routes(logging.WithLogger(signals.SetupSignalHandler(), log), router, sysInfo, authInfo, "/", authRouteMap)
	t.Run("/404", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, err := http.NewRequest(http.MethodGet, "/404", nil)
		require.NoError(t, err)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("/livez", func(t *testing.T) {
		w := httptest.NewRecorder()
		req, err := http.NewRequest(http.MethodGet, "/livez", nil)
		require.NoError(t, err)
		router.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}
