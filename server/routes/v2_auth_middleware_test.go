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

package routes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/authz"
	"github.com/numaproj/numaflow/server/common"
)

func TestV2AuthMiddlewareReturnsProblemForMissingLoginCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	routeMap := authz.RouteMap{"GET:/api/v2/capabilities": authz.NewRouteInfo(authz.ObjectAll, false)}
	router := gin.New()
	router.Use(v2AuthMiddleware(context.Background(), &mockAuthorizer{}, &mockAuthenticator{}, &mockAuthenticator{}, routeMap))
	router.GET("/api/v2/capabilities", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v2/capabilities", nil))

	assert.Equal(t, http.StatusUnauthorized, recorder.Code)
	assert.Equal(t, "application/problem+json", recorder.Header().Get("Content-Type"))
	var problem map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
	assert.Equal(t, "authentication_failed", problem["code"])
	assert.Equal(t, "/api/v2/problems/authentication_failed", problem["type"])
}

func TestV2AuthMiddlewareAuthenticatesCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	user := createTestUserInfo("user@example.com", nil)
	dexAuth := &mockAuthenticator{authenticateFunc: func(*gin.Context) (*authn.UserInfo, error) {
		return user, nil
	}}
	routeMap := authz.RouteMap{"GET:/api/v2/capabilities": authz.NewRouteInfo(authz.ObjectAll, false)}
	router := gin.New()
	router.Use(v2AuthMiddleware(context.Background(), &mockAuthorizer{}, dexAuth, &mockAuthenticator{}, routeMap))
	router.GET("/api/v2/capabilities", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/api/v2/capabilities", nil)
	request.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "dex"})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusNoContent, recorder.Code)
}

func TestV2AuthMiddlewareRejectsUnknownRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	user := createTestUserInfo("user@example.com", nil)
	dexAuth := &mockAuthenticator{authenticateFunc: func(*gin.Context) (*authn.UserInfo, error) {
		return user, nil
	}}
	router := gin.New()
	router.Use(v2AuthMiddleware(context.Background(), &mockAuthorizer{}, dexAuth, &mockAuthenticator{}, authz.RouteMap{}))
	router.GET("/api/v2/capabilities", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/api/v2/capabilities", nil)
	request.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "dex"})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusForbidden, recorder.Code)
	var problem map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
	assert.Equal(t, "route_not_authorized", problem["code"])
}

func TestV2AuthMiddlewareRejectsUnauthorizedUser(t *testing.T) {
	gin.SetMode(gin.TestMode)
	user := createTestUserInfo("user@example.com", nil)
	dexAuth := &mockAuthenticator{authenticateFunc: func(*gin.Context) (*authn.UserInfo, error) {
		return user, nil
	}}
	routeMap := authz.RouteMap{"GET:/api/v2/capabilities": authz.NewRouteInfo(authz.ObjectAll, true)}
	router := gin.New()
	router.Use(v2AuthMiddleware(context.Background(), &mockAuthorizer{}, dexAuth, &mockAuthenticator{}, routeMap))
	router.GET("/api/v2/capabilities", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/api/v2/capabilities", nil)
	request.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "dex"})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusForbidden, recorder.Code)
	var problem map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
	assert.Equal(t, "authorization_denied", problem["code"])
}
