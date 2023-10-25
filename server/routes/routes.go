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
	"fmt"
	"net/http"

	"github.com/casbin/casbin/v2"
	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	v1 "github.com/numaproj/numaflow/server/apis/v1"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/authz"
	"github.com/numaproj/numaflow/server/common"
	"github.com/numaproj/numaflow/server/utils"
)

type SystemInfo struct {
	ManagedNamespace string `json:"managedNamespace"`
	Namespaced       bool   `json:"namespaced"`
	Version          string `json:"version"`
}

type AuthInfo struct {
	DisableAuth   bool   `json:"disableAuth"`
	DexServerAddr string `json:"dexServerAddr"`
	DexProxyAddr  string `json:"dexProxyAddr"`
	ServerAddr    string `json:"serverAddr"`
}

var logger = logging.NewLogger().Named("server")

func Routes(r *gin.Engine, sysInfo SystemInfo, authInfo AuthInfo) {
	r.GET("/livez", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	// noAuthGroup is a group of routes that do not require AuthN/AuthZ no matter whether auth is enabled.
	noAuthGroup := r.Group("/auth/v1")
	v1RoutesNoAuth(noAuthGroup, authInfo)

	// r1Group is a group of routes that require AuthN/AuthZ when auth is enabled.
	// they share the AuthN/AuthZ middleware.
	r1Group := r.Group("/api/v1")
	if !authInfo.DisableAuth {
		enforcer, err := authz.GetEnforcer()
		if err != nil {
			panic(err)
		}
		dexObj := v1.NewDexObject(authInfo.ServerAddr, authInfo.DexProxyAddr)
		if dexObj == nil {
			panic(fmt.Errorf("failed to create DexObject"))
		}
		// Add the AuthN/AuthZ middleware to the group.
		r1Group.Use(authMiddleware(enforcer, dexObj))
	}
	v1Routes(r1Group)
	r1Group.GET("/sysinfo", func(c *gin.Context) {
		c.JSON(http.StatusOK, v1.NewNumaflowAPIResponse(nil, sysInfo))
	})
}

func v1RoutesNoAuth(r gin.IRouter, authInfo AuthInfo) {
	handler, err := v1.NewNoAuthHandler(authInfo.ServerAddr, authInfo.DexProxyAddr)
	if err != nil {
		panic(err)
	}
	// Handle the login request.
	r.GET("/login", handler.Login)
	// Handle the logout request.
	r.GET("/logout", handler.Logout)
	// Handle the callback request.
	r.GET("/callback", handler.Callback)
}

// v1Routes defines the routes for the v1 API. For adding a new route, add a new handler function
// for the route along with an entry in the RouteMap in auth/route_map.go.
func v1Routes(r gin.IRouter) {
	handler, err := v1.NewHandler()
	if err != nil {
		panic(err)
	}
	// Handle the authinfo request.
	r.GET("/authinfo", handler.AuthInfo)
	// List all namespaces that have Pipeline or InterStepBufferService objects.
	r.GET("/namespaces", handler.ListNamespaces)
	// Summarized information of all the namespaces in a cluster wrapped in a list.
	r.GET("/cluster-summary", handler.GetClusterSummary)
	// Create a Pipeline.
	r.POST("/namespaces/:namespace/pipelines", handler.CreatePipeline)
	// All pipelines for a given namespace.
	r.GET("/namespaces/:namespace/pipelines", handler.ListPipelines)
	// Get a Pipeline information.
	r.GET("/namespaces/:namespace/pipelines/:pipeline", handler.GetPipeline)
	// Get a Pipeline health information.
	r.GET("/namespaces/:namespace/pipelines/:pipeline/health", handler.GetPipelineStatus)
	// Update a Pipeline.
	r.PUT("/namespaces/:namespace/pipelines/:pipeline", handler.UpdatePipeline)
	// Delete a Pipeline.
	r.DELETE("/namespaces/:namespace/pipelines/:pipeline", handler.DeletePipeline)
	// Patch the pipeline spec to achieve operations such as "pause" and "resume".
	r.PATCH("/namespaces/:namespace/pipelines/:pipeline", handler.PatchPipeline)
	// Create an InterStepBufferService object.
	r.POST("/namespaces/:namespace/isb-services", handler.CreateInterStepBufferService)
	// List all the InterStepBufferService objects for a given namespace.
	r.GET("/namespaces/:namespace/isb-services", handler.ListInterStepBufferServices)
	// Get an InterStepBufferService object.
	r.GET("/namespaces/:namespace/isb-services/:isb-service", handler.GetInterStepBufferService)
	// Update an InterStepBufferService object.
	r.PUT("/namespaces/:namespace/isb-services/:isb-service", handler.UpdateInterStepBufferService)
	// Delete an InterStepBufferService object.
	r.DELETE("/namespaces/:namespace/isb-services/:isb-service", handler.DeleteInterStepBufferService)
	// Get all the Inter-Step Buffers of a pipeline.
	r.GET("/namespaces/:namespace/pipelines/:pipeline/isbs", handler.ListPipelineBuffers)
	// Get all the watermarks information of a pipeline.
	r.GET("/namespaces/:namespace/pipelines/:pipeline/watermarks", handler.GetPipelineWatermarks)
	// Update a vertex spec.
	r.PUT("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex", handler.UpdateVertex)
	// Get all the vertex metrics of a pipeline.
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/metrics", handler.GetVerticesMetrics)
	// Get all the pods of a vertex.
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods", handler.ListVertexPods)
	// Get the metrics such as cpu, memory usage for a pod.
	r.GET("/metrics/namespaces/:namespace/pods", handler.ListPodsMetrics)
	// Get pod logs.
	r.GET("/namespaces/:namespace/pods/:pod/logs", handler.PodLogs)
	// List of the Kubernetes events of a namespace.
	r.GET("/namespaces/:namespace/events", handler.GetNamespaceEvents)

}

func authMiddleware(enforcer *casbin.Enforcer, dexObj *v1.DexObject) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Authenticate the user.
		userIdentityToken, err := authenticate(c, dexObj)
		if err != nil {
			errMsg := fmt.Sprintf("failed to authenticate user: %v", err)
			c.JSON(http.StatusUnauthorized, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
			return
		}
		// Authorize the user and the request.
		// Get the user from the user identity token.
		groups := userIdentityToken.IDTokenClaims.Groups
		resource := authz.ExtractResource(c)
		object := authz.ExtractObject(c)
		action := c.Request.Method
		isAuthorized := false

		// Get the route map from the context. Key is in the format "method:path".
		routeMapKey := fmt.Sprintf("%s:%s", action, c.FullPath())
		// Check if the route requires auth.
		if authz.RouteMap[routeMapKey] != nil && authz.RouteMap[routeMapKey].RequiresAuth {
			// Check if the user has permission for any of the groups.
			for _, group := range groups {
				// Get the user from the group. The group is in the format "group:role".
				// Check if the user has permission using Casbin Enforcer.
				if enforceRBAC(enforcer, group, resource, object, action) {
					isAuthorized = true
					c.Next()
					break
				}
			}
			// If the user is not authorized, return an error.
			if !isAuthorized {
				errMsg := "user is not authorized to execute the requested action."
				c.JSON(http.StatusForbidden, v1.NewNumaflowAPIResponse(&errMsg, nil))
				c.Abort()
			}
		} else if authz.RouteMap[routeMapKey] != nil && !authz.RouteMap[routeMapKey].RequiresAuth {
			// If the route does not require auth, skip the authz check.
			c.Next()
		} else {
			// If the route is not present in the route map, return an error.
			logger.Errorw("route not present in routeMap", "route", routeMapKey)
			errMsg := "invalid route"
			c.JSON(http.StatusForbidden, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
			return
		}
	}
}

func authenticate(c *gin.Context, dexObj *v1.DexObject) (*authn.UserIdInfo, error) {
	userIdentityTokenStr, err := c.Cookie(common.UserIdentityCookieName)
	if err != nil {
		return nil, fmt.Errorf("failed to get user identity token from cookie: %v", err)
	}
	userIdentityToken, err := utils.ParseUserIdentityToken(userIdentityTokenStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse user identity token: %v", err)
	}
	_, err = dexObj.Verify(c, userIdentityToken.IDToken)
	if err != nil {
		return nil, err
	}
	return userIdentityToken, nil
}

// enforceRBAC checks if the user has permission based on the Casbin model and policy.
func enforceRBAC(enforcer *casbin.Enforcer, user, resource, object, action string) bool {
	ok, _ := enforcer.Enforce(user, resource, object, action)
	return ok
}
