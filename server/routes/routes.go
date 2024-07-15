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
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/numaproj/numaflow/pkg/shared/logging"
	v1 "github.com/numaproj/numaflow/server/apis/v1"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/authz"
	"github.com/numaproj/numaflow/server/common"
)

type SystemInfo struct {
	ManagedNamespace     string `json:"managedNamespace"`
	Namespaced           bool   `json:"namespaced"`
	IsReadOnly           bool   `json:"isReadOnly"`
	Version              string `json:"version"`
	DaemonClientProtocol string `json:"daemonClientProtocol"`
}

type AuthInfo struct {
	DisableAuth   bool   `json:"disableAuth"`
	DexServerAddr string `json:"dexServerAddr"`
	ServerAddr    string `json:"serverAddr"`
}

func Routes(ctx context.Context, r *gin.Engine, sysInfo SystemInfo, authInfo AuthInfo, baseHref string, authRouteMap authz.RouteMap) {
	r.GET("/livez", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	dexObj, err := v1.NewDexObject(authInfo.ServerAddr, baseHref, authInfo.DexServerAddr)
	if err != nil {
		panic(err)
	}

	localUsersAuthObj, err := v1.NewLocalUsersAuthObject(ctx, authInfo.DisableAuth)
	if err != nil {
		panic(err)
	}

	// noAuthGroup is a group of routes that do not require AuthN/AuthZ no matter whether auth is enabled.
	noAuthGroup := r.Group(baseHref + "auth/v1")
	v1RoutesNoAuth(noAuthGroup, dexObj, localUsersAuthObj)

	// r1Group is a group of routes that require AuthN/AuthZ when auth is enabled.
	// they share the AuthN/AuthZ middleware.
	r1Group := r.Group(baseHref + "api/v1")
	if !authInfo.DisableAuth {
		authorizer, err := authz.NewCasbinObject(ctx, authRouteMap)
		if err != nil {
			panic(err)
		}
		// Add the AuthN/AuthZ middleware to the group.
		r1Group.Use(authMiddleware(ctx, authorizer, dexObj, localUsersAuthObj, authRouteMap))
		v1Routes(ctx, r1Group, dexObj, localUsersAuthObj, sysInfo.IsReadOnly, sysInfo.DaemonClientProtocol)
	} else {
		v1Routes(ctx, r1Group, nil, nil, sysInfo.IsReadOnly, sysInfo.DaemonClientProtocol)
	}
	r1Group.GET("/sysinfo", func(c *gin.Context) {
		c.JSON(http.StatusOK, v1.NewNumaflowAPIResponse(nil, sysInfo))
	})
}

func v1RoutesNoAuth(r gin.IRouter, dexObj *v1.DexObject, localUsersAuthObject *v1.LocalUsersAuthObject) {
	handler, err := v1.NewNoAuthHandler(dexObj, localUsersAuthObject)
	if err != nil {
		panic(err)
	}
	// Handle the login request.
	r.GET("/login", handler.Login)
	// Handle the login request for local users.
	r.POST("/login", handler.LoginLocalUsers)
	// Handle the logout request.
	r.GET("/logout", handler.Logout)
	// Handle the callback request.
	r.GET("/callback", handler.Callback)
}

// v1Routes defines the routes for the v1 API. For adding a new route, add a new handler function
// for the route along with an entry in the RouteMap in auth/route_map.go.
func v1Routes(ctx context.Context, r gin.IRouter, dexObj *v1.DexObject, localUsersAuthObject *v1.LocalUsersAuthObject, isReadOnly bool, daemonClientProtocol string) {
	handlerOpts := []v1.HandlerOption{v1.WithDaemonClientProtocol(daemonClientProtocol)}
	if isReadOnly {
		handlerOpts = append(handlerOpts, v1.WithReadOnlyMode())
	}
	handler, err := v1.NewHandler(ctx, dexObj, localUsersAuthObject, handlerOpts...)
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

// authMiddleware is the middleware for AuthN/AuthZ.
// it ensures the user is authenticated and authorized
// to execute the requested action before sending the request to the api handler.
func authMiddleware(ctx context.Context, authorizer authz.Authorizer, dexAuthenticator authn.Authenticator, localUsersAuthenticator authn.Authenticator, authRouteMap authz.RouteMap) gin.HandlerFunc {

	return func(c *gin.Context) {

		log := logging.FromContext(ctx)
		var userInfo *authn.UserInfo

		loginType, err := c.Cookie(common.LoginCookieName)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get login type: %v", err)
			c.JSON(http.StatusUnauthorized, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
			return
		}

		// Authenticate the user based on the login type.
		if loginType == "dex" {
			userInfo, err = dexAuthenticator.Authenticate(c)
		} else if loginType == "local" {
			userInfo, err = localUsersAuthenticator.Authenticate(c)
		} else {
			errMsg := fmt.Sprintf("unidentified login type received: %v", loginType)
			c.JSON(http.StatusUnauthorized, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
			return
		}

		if err != nil {
			errMsg := fmt.Sprintf("Failed to authenticate user: %v", err)
			c.JSON(http.StatusUnauthorized, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
			return
		}
		// Check if the route requires authorization.
		if authRouteMap.GetRouteFromContext(c) != nil && authRouteMap.GetRouteFromContext(c).RequiresAuthZ {
			// Check if the user is authorized to execute the requested action.
			isAuthorized := authorizer.Authorize(c, userInfo)
			if isAuthorized {
				// If the user is authorized, continue the request.
				c.Next()
			} else {
				// If the user is not authorized, return an error.
				errMsg := "user is not authorized to execute the requested action"
				c.JSON(http.StatusForbidden, v1.NewNumaflowAPIResponse(&errMsg, nil))
				c.Abort()
			}
		} else if authRouteMap.GetRouteFromContext(c) != nil && !authRouteMap.GetRouteFromContext(c).RequiresAuthZ {
			// If the route does not require AuthZ, skip the AuthZ check.
			c.Next()
		} else {
			// If the route is not present in the route map, return an error.
			log.Errorw("route not present in routeMap", "route", authz.GetRouteMapKey(c))
			errMsg := "Invalid route"
			c.JSON(http.StatusForbidden, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
		}
	}
}
