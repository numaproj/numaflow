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

	"github.com/gin-gonic/gin"

	v1 "github.com/numaproj/numaflow/server/apis/v1"
)

func Routes(r *gin.Engine) {
	r.GET("/healthz", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	v1Routes(r.Group("/api/v1"))
}

func v1Routes(r gin.IRouter) {
	handler, err := v1.NewHandler()
	if err != nil {
		panic(err)
	}
	r.GET("/namespaces", handler.ListNamespaces)
	r.GET("/namespaces/:namespace/pipelines", handler.ListPipelines)
	r.GET("/namespaces/:namespace/isbsvcs", handler.ListInterStepBufferServices)
	r.GET("/namespaces/:namespace/isbsvcs/:isbsvc", handler.GetInterStepBufferService)
	r.GET("/namespaces/:namespace/pipelines/:pipeline", handler.GetPipeline)
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices", handler.ListVertices)
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex", handler.GetVertex)
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/pods", handler.ListVertexPods)
	r.GET("/namespaces/:namespace/pods/:pod/log", handler.PodLogs)
	r.GET("/metrics/namespaces/:namespace/pods", handler.ListPodsMetrics)
	r.GET("/metrics/namespaces/:namespace/pods/:pod", handler.GetPodMetrics)
	r.GET("/namespaces/:namespace/pipelines/:pipeline/edges", handler.ListPipelineEdges)
	r.GET("/namespaces/:namespace/pipelines/:pipeline/edges/:edge", handler.GetPipelineEdge)
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/metrics", handler.GetVertexMetrics)
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/watermark", handler.GetVertexWatermark)
}
