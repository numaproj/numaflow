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
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/metrics", handler.GetVertexMetricsAndWatermark)
	r.GET("/namespaces/:namespace/pipelines/:pipeline/vertices/:vertex/watermark", handler.GetVertexWatermark)
}
