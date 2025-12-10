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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/authz"
	"github.com/numaproj/numaflow/server/common"
)

// mockAuthorizer is a mock implementation of the Authorizer interface
type mockAuthorizer struct {
	authorizeFunc func(c *gin.Context, userInfo *authn.UserInfo) bool
}

func (m *mockAuthorizer) Authorize(c *gin.Context, userInfo *authn.UserInfo) bool {
	if m.authorizeFunc != nil {
		return m.authorizeFunc(c, userInfo)
	}
	return false
}

// mockAuthenticator is a mock implementation of the Authenticator interface
type mockAuthenticator struct {
	authenticateFunc func(c *gin.Context) (*authn.UserInfo, error)
}

func (m *mockAuthenticator) Authenticate(c *gin.Context) (*authn.UserInfo, error) {
	if m.authenticateFunc != nil {
		return m.authenticateFunc(c)
	}
	return nil, errors.New("authentication failed")
}

// Helper function to create a test user info
func createTestUserInfo(email string, groups []string) *authn.UserInfo {
	return &authn.UserInfo{
		IDTokenClaims: &authn.IDTokenClaims{
			Email:  email,
			Groups: groups,
		},
		IDToken:      "test-token",
		RefreshToken: "test-refresh-token",
	}
}

func TestAuthMiddleware_MissingLoginCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	authorizer := &mockAuthorizer{}
	dexAuth := &mockAuthenticator{}
	localAuth := &mockAuthenticator{}
	routeMap := authz.RouteMap{}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["errMsg"], "Failed to get login type")
}

func TestAuthMiddleware_InvalidLoginType(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	authorizer := &mockAuthorizer{}
	dexAuth := &mockAuthenticator{}
	localAuth := &mockAuthenticator{}
	routeMap := authz.RouteMap{}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  common.LoginCookieName,
		Value: "invalid-type",
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["errMsg"], "unidentified login type")
}

func TestAuthMiddleware_DexAuthenticationSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	testUser := createTestUserInfo("test@example.com", []string{"admin"})

	authorizer := &mockAuthorizer{
		authorizeFunc: func(c *gin.Context, userInfo *authn.UserInfo) bool {
			return true
		},
	}
	dexAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return testUser, nil
		},
	}
	localAuth := &mockAuthenticator{}

	routeMap := authz.RouteMap{
		"GET:/test": authz.NewRouteInfo("test-object", true),
	}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  common.LoginCookieName,
		Value: "dex",
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuthMiddleware_LocalAuthenticationSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	testUser := createTestUserInfo("admin@local", []string{"admin"})

	authorizer := &mockAuthorizer{
		authorizeFunc: func(c *gin.Context, userInfo *authn.UserInfo) bool {
			return true
		},
	}
	dexAuth := &mockAuthenticator{}
	localAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return testUser, nil
		},
	}

	routeMap := authz.RouteMap{
		"GET:/test": authz.NewRouteInfo("test-object", true),
	}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  common.LoginCookieName,
		Value: "local",
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuthMiddleware_DexAuthenticationFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	authorizer := &mockAuthorizer{}
	dexAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return nil, errors.New("invalid token")
		},
	}
	localAuth := &mockAuthenticator{}
	routeMap := authz.RouteMap{}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  common.LoginCookieName,
		Value: "dex",
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["errMsg"], "Failed to authenticate user")
}

func TestAuthMiddleware_LocalAuthenticationFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	authorizer := &mockAuthorizer{}
	dexAuth := &mockAuthenticator{}
	localAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return nil, errors.New("invalid credentials")
		},
	}
	routeMap := authz.RouteMap{}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  common.LoginCookieName,
		Value: "local",
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["errMsg"], "Failed to authenticate user")
}

func TestAuthMiddleware_AuthorizationRequired_Authorized(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	testUser := createTestUserInfo("test@example.com", []string{"admin"})

	authorizer := &mockAuthorizer{
		authorizeFunc: func(c *gin.Context, userInfo *authn.UserInfo) bool {
			assert.Equal(t, testUser, userInfo)
			return true
		},
	}
	dexAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return testUser, nil
		},
	}
	localAuth := &mockAuthenticator{}

	routeMap := authz.RouteMap{
		"POST:/api/v1/pipelines": authz.NewRouteInfo("pipeline", true),
	}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.POST("/api/v1/pipelines", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "created"})
	})

	req, _ := http.NewRequest(http.MethodPost, "/api/v1/pipelines", nil)
	req.AddCookie(&http.Cookie{
		Name:  common.LoginCookieName,
		Value: "dex",
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuthMiddleware_AuthorizationRequired_NotAuthorized(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	testUser := createTestUserInfo("test@example.com", []string{"readonly"})

	authorizer := &mockAuthorizer{
		authorizeFunc: func(c *gin.Context, userInfo *authn.UserInfo) bool {
			return false
		},
	}
	dexAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return testUser, nil
		},
	}
	localAuth := &mockAuthenticator{}

	routeMap := authz.RouteMap{
		"POST:/api/v1/pipelines": authz.NewRouteInfo("pipeline", true),
	}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.POST("/api/v1/pipelines", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "created"})
	})

	req, _ := http.NewRequest(http.MethodPost, "/api/v1/pipelines", nil)
	req.AddCookie(&http.Cookie{
		Name:  common.LoginCookieName,
		Value: "dex",
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["errMsg"], "user is not authorized")
}

func TestAuthMiddleware_NoAuthorizationRequired(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	testUser := createTestUserInfo("test@example.com", []string{"readonly"})

	// Authorizer should not be called
	authorizer := &mockAuthorizer{
		authorizeFunc: func(c *gin.Context, userInfo *authn.UserInfo) bool {
			t.Fatal("authorizer should not be called for routes without AuthZ")
			return false
		},
	}
	dexAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return testUser, nil
		},
	}
	localAuth := &mockAuthenticator{}

	routeMap := authz.RouteMap{
		"GET:/api/v1/sysinfo": authz.NewRouteInfo("sysinfo", false),
	}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/api/v1/sysinfo", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"version": "1.0.0"})
	})

	req, _ := http.NewRequest(http.MethodGet, "/api/v1/sysinfo", nil)
	req.AddCookie(&http.Cookie{
		Name:  common.LoginCookieName,
		Value: "dex",
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestAuthMiddleware_RouteNotInMap(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	testUser := createTestUserInfo("test@example.com", []string{"admin"})

	authorizer := &mockAuthorizer{}
	dexAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return testUser, nil
		},
	}
	localAuth := &mockAuthenticator{}

	// Empty route map - route will not be found
	routeMap := authz.RouteMap{}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/unknown-route", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req, _ := http.NewRequest(http.MethodGet, "/unknown-route", nil)
	req.AddCookie(&http.Cookie{
		Name:  common.LoginCookieName,
		Value: "dex",
	})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusForbidden, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["errMsg"], "Invalid route")
}

func TestAuthMiddleware_MultipleRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	adminUser := createTestUserInfo("admin@example.com", []string{"admin"})
	readonlyUser := createTestUserInfo("user@example.com", []string{"readonly"})

	authorizer := &mockAuthorizer{
		authorizeFunc: func(c *gin.Context, userInfo *authn.UserInfo) bool {
			// Admin can POST, readonly cannot
			if c.Request.Method == "POST" {
				return userInfo.IDTokenClaims.Email == "admin@example.com"
			}
			// Both can GET
			return true
		},
	}

	dexAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			// Determine user from a custom header for testing
			userType := c.GetHeader("X-Test-User")
			if userType == "admin" {
				return adminUser, nil
			}
			return readonlyUser, nil
		},
	}
	localAuth := &mockAuthenticator{}

	routeMap := authz.RouteMap{
		"GET:/api/v1/data":  authz.NewRouteInfo("data", true),
		"POST:/api/v1/data": authz.NewRouteInfo("data", true),
	}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/api/v1/data", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"data": "test"})
	})
	router.POST("/api/v1/data", func(c *gin.Context) {
		c.JSON(http.StatusCreated, gin.H{"status": "created"})
	})

	// Test 1: Admin GET - should succeed
	req1, _ := http.NewRequest(http.MethodGet, "/api/v1/data", nil)
	req1.Header.Set("X-Test-User", "admin")
	req1.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "dex"})
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	assert.Equal(t, http.StatusOK, w1.Code)

	// Test 2: Readonly GET - should succeed
	req2, _ := http.NewRequest(http.MethodGet, "/api/v1/data", nil)
	req2.Header.Set("X-Test-User", "readonly")
	req2.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "dex"})
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)

	// Test 3: Admin POST - should succeed
	req3, _ := http.NewRequest(http.MethodPost, "/api/v1/data", nil)
	req3.Header.Set("X-Test-User", "admin")
	req3.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "dex"})
	w3 := httptest.NewRecorder()
	router.ServeHTTP(w3, req3)
	assert.Equal(t, http.StatusCreated, w3.Code)

	// Test 4: Readonly POST - should fail
	req4, _ := http.NewRequest(http.MethodPost, "/api/v1/data", nil)
	req4.Header.Set("X-Test-User", "readonly")
	req4.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "dex"})
	w4 := httptest.NewRecorder()
	router.ServeHTTP(w4, req4)
	assert.Equal(t, http.StatusForbidden, w4.Code)
}

func TestAuthMiddleware_HandlerChainContinues(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	testUser := createTestUserInfo("test@example.com", []string{"admin"})

	var middlewareCalled bool
	var handlerCalled bool

	authorizer := &mockAuthorizer{
		authorizeFunc: func(c *gin.Context, userInfo *authn.UserInfo) bool {
			return true
		},
	}
	dexAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return testUser, nil
		},
	}
	localAuth := &mockAuthenticator{}

	routeMap := authz.RouteMap{
		"GET:/test": authz.NewRouteInfo("test", true),
	}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.Use(func(c *gin.Context) {
		middlewareCalled = true
		c.Next()
	})
	router.GET("/test", func(c *gin.Context) {
		handlerCalled = true
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	req, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "dex"})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, middlewareCalled)
	assert.True(t, handlerCalled)
}

func TestAuthMiddleware_BothAuthTypesWork(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx := context.Background()
	dexUser := createTestUserInfo("dex@example.com", []string{"admin"})
	localUser := createTestUserInfo("local@example.com", []string{"admin"})

	authorizer := &mockAuthorizer{
		authorizeFunc: func(c *gin.Context, userInfo *authn.UserInfo) bool {
			return true
		},
	}
	dexAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return dexUser, nil
		},
	}
	localAuth := &mockAuthenticator{
		authenticateFunc: func(c *gin.Context) (*authn.UserInfo, error) {
			return localUser, nil
		},
	}

	routeMap := authz.RouteMap{
		"GET:/test": authz.NewRouteInfo("test", true),
	}

	router := gin.New()
	router.Use(authMiddleware(ctx, authorizer, dexAuth, localAuth, routeMap))
	router.GET("/test", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	// Test with dex
	req1, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req1.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "dex"})
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, req1)
	assert.Equal(t, http.StatusOK, w1.Code)

	// Test with local
	req2, _ := http.NewRequest(http.MethodGet, "/test", nil)
	req2.AddCookie(&http.Cookie{Name: common.LoginCookieName, Value: "local"})
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, req2)
	assert.Equal(t, http.StatusOK, w2.Code)
}
