package apis

import "github.com/gin-gonic/gin"

type Handler interface {
	ListPipelines(c *gin.Context)
	GetPipeline(c *gin.Context)
	ListNamespaces(c *gin.Context)
	ListInterStepBufferServices(c *gin.Context)
	GetInterStepBufferService(c *gin.Context)
	ListVertices(c *gin.Context)
	GetVertex(c *gin.Context)
	GetVertexMetrics(c *gin.Context)
	ListVertexPods(c *gin.Context)
	PodLogs(c *gin.Context)
	ListPodsMetrics(c *gin.Context)
	GetPodMetrics(c *gin.Context)
	ListPipelineEdges(c *gin.Context)
	GetPipelineEdge(c *gin.Context)
}
