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
	"net/http"

	"github.com/gin-gonic/gin"

	v1 "github.com/numaproj/numaflow/server/apis/v1"
	"github.com/numaproj/numaflow/server/authz"
)

type SystemInfo struct {
	ManagedNamespace     string `json:"managedNamespace"`
	Namespaced           bool   `json:"namespaced"`
	IsReadOnly           bool   `json:"isReadOnly"`
	DisableMetricsCharts bool   `json:"disableMetricsCharts"`
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

	// promql service instance.
	promQlServiceObj, err := v1.NewPromQlServiceObject()
	if err != nil {
		panic(err)
	}

	// disable metrics charts if metric config or prometheus client is not set.
	sysInfo.DisableMetricsCharts = promQlServiceObj.DisableMetricsChart()

	// noAuthGroup is a group of routes that do not require AuthN/AuthZ no matter whether auth is enabled.
	noAuthGroup := r.Group(baseHref + "auth/v1")
	v1RoutesNoAuth(noAuthGroup, dexObj, localUsersAuthObj)

	// r1Group is a group of routes that require AuthN/AuthZ when auth is enabled.
	// they share the AuthN/AuthZ middleware.
	r1Group := r.Group(baseHref + "api/v1")
	r1Group.Use(cleanResponseMiddleware())
	if !authInfo.DisableAuth {
		authorizer, err := authz.NewCasbinObject(ctx, authRouteMap)
		if err != nil {
			panic(err)
		}
		// Add the AuthN/AuthZ middleware to the group.
		r1Group.Use(authMiddleware(ctx, authorizer, dexObj, localUsersAuthObj, authRouteMap))
		v1Routes(ctx, r1Group, dexObj, localUsersAuthObj, promQlServiceObj, sysInfo.IsReadOnly, sysInfo.DaemonClientProtocol)
	} else {
		v1Routes(ctx, r1Group, nil, nil, promQlServiceObj, sysInfo.IsReadOnly, sysInfo.DaemonClientProtocol)
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
func v1Routes(ctx context.Context, r gin.IRouter, dexObj *v1.DexObject, localUsersAuthObject *v1.LocalUsersAuthObject, promQlServiceObj v1.PromQl, isReadOnly bool, daemonClientProtocol string) {
	handlerOpts := []v1.HandlerOption{v1.WithDaemonClientProtocol(daemonClientProtocol)}
	if isReadOnly {
		handlerOpts = append(handlerOpts, v1.WithReadOnlyMode())
	}
	handler, err := v1.NewHandler(ctx, dexObj, localUsersAuthObject, promQlServiceObj, handlerOpts...)
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
	// List all pipelines for a given namespace.
	r.GET("/namespaces/:namespace/pipelines", handler.ListPipelines)
	// Get the pipeline information.
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
	// Get the pod metrics for a mono vertex.
	r.GET("/namespaces/:namespace/mono-vertices/:mono-vertex/pods-info", handler.GetMonoVertexPodsInfo)
	// Get the pod metrics for a pipeline vertex.
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods-info", handler.GetVertexPodsInfo)
	// List of the Kubernetes events of a namespace.
	r.GET("/namespaces/:namespace/events", handler.GetNamespaceEvents)
	// List all mono vertices for a given namespace.
	r.GET("/namespaces/:namespace/mono-vertices", handler.ListMonoVertices)
	// Get the mono vertex information.
	r.GET("/namespaces/:namespace/mono-vertices/:mono-vertex", handler.GetMonoVertex)
	// Delete a mono-vertex.
	r.DELETE("/namespaces/:namespace/mono-vertices/:mono-vertex", handler.DeleteMonoVertex)
	// Get all the pods of a mono vertex.
	r.GET("/namespaces/:namespace/mono-vertices/:mono-vertex/pods", handler.ListMonoVertexPods)
	// Create a mono vertex.
	r.POST("/namespaces/:namespace/mono-vertices", handler.CreateMonoVertex)
	// Get the metrics of a mono vertex.
	r.GET("/namespaces/:namespace/mono-vertices/:mono-vertex/metrics", handler.GetMonoVertexMetrics)
	// Get the health information of a mono vertex.
	r.GET("/namespaces/:namespace/mono-vertices/:mono-vertex/health", handler.GetMonoVertexHealth)
	// Get the errors for a given pipeline vertex replica.
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/errors", handler.GetVertexErrors)
	// Get the errors for a given mono vertex replica.
	r.GET("/namespaces/:namespace/mono-vertices/:mono-vertex/errors", handler.GetMonoVertexErrors)
	// Get the time series data across different dimensions.
	r.POST("/metrics-proxy", handler.GetMetricData)
	// Discover the metrics for a given object type.
	r.GET("/metrics-discovery/object/:object", handler.DiscoverMetrics)
}
