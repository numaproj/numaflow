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
	"strings"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/gin-gonic/gin"

	v1 "github.com/numaproj/numaflow/server/apis/v1"
)

type SystemInfo struct {
	ManagedNamespace string `json:"managedNamespace"`
	Namespaced       bool   `json:"namespaced"`
	// TODO: Get the version of the current Numaflow
	Version string `json:"version"`
}

func Routes(r *gin.Engine, sysinfo SystemInfo) {
	r.GET("/livez", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})

	r.Any("/dex/*name", v1.DexReverseProxy)
	enforcer, err := getEnforcer()
	if err != nil {

	}
	r1Group := r.Group("/api/v1")
	v1Routes(r1Group)
	r1Group.Use(func(c *gin.Context) {
		userIdentityTokenStr, err := c.Cookie("user-identity-token")
		if err != nil {
			errMsg := "user is not authenticated."
			c.JSON(http.StatusUnauthorized, v1.NewNumaflowAPIResponse(&errMsg, nil))
			return
		}
		userIdentityToken := v1.GetUserIdentityToken(userIdentityTokenStr)
		groups := userIdentityToken.IDTokenClaims.Groups
		// user := c.DefaultQuery("user", "readonly")
		// user := "admin" // Replace with actual user from your authentication system.
		resource := c.FullPath()
		action := c.Request.Method
		auth := false

		for _, group := range groups {
			user := strings.Split(group, ":")[1]
			// Check if the user has permission using Casbin Enforcer.
			if enforceRBAC(enforcer, user, resource, action) {
				auth = true
				c.Next()
			}
		}
		if !auth {
			errMsg := "user is not authorized to execute the requested action."
			c.JSON(http.StatusForbidden, v1.NewNumaflowAPIResponse(&errMsg, nil))
			c.Abort()
		}
	})
	r1Group.GET("/sysinfo", func(c *gin.Context) {
		c.JSON(http.StatusOK, v1.NewNumaflowAPIResponse(nil, sysinfo))
	})
	noAuthGroup := r.Group("/auth/v1")
	v1RoutesNoAuth(noAuthGroup)
}

func v1RoutesNoAuth(r gin.IRouter) {
	handler, err := v1.NewHandler()
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

func getEnforcer() (*casbin.Enforcer, error) {
	modelText := `
	[request_definition]
	r = sub, obj, act
	
	[policy_definition]
	p = sub, obj, act
	
	[role_definition]
	g = _, _
	
	[policy_effect]
	e = some(where (p.eft == allow))
	
	[matchers]
	m = g(r.sub, p.sub) && keyMatch(r.obj,p.obj) && r.act == p.act
	`

	// Initialize the Casbin model from the model configuration string.
	model, err := model.NewModelFromString(modelText)
	if err != nil {
		return nil, err
	}

	// Initialize the Casbin Enforcer with the model.
	enforcer, err := casbin.NewEnforcer(model)
	if err != nil {
		return nil, err
	}
	rules := [][]string{
		[]string{"role:readonly", "*", "GET"},
		[]string{"role:admin", "*", "POST"},
		[]string{"role:admin", "*", "PUT"},
		[]string{"role:admin", "*", "UPDATE"},
		[]string{"role:admin", "*", "DELETE"},
	}

	areRulesAdded, err := enforcer.AddPolicies(rules)
	if !areRulesAdded {
		return nil, err
	}

	rulesGroup := [][]string{
		[]string{"role:admin", "role:readonly"},
		[]string{"readonly", "role:readonly"},
		[]string{"admin", "role:admin"},
	}

	areRulesAdded, err = enforcer.AddNamedGroupingPolicies("g", rulesGroup)
	if !areRulesAdded {
		return nil, err
	}
	return enforcer, nil
}

// enforceRBAC checks if the user has permission based on the Casbin model and policy.
func enforceRBAC(enforcer *casbin.Enforcer, user, resource, action string) bool {
	ok, _ := enforcer.Enforce(user, resource, action)
	return ok
}
