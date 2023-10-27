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

package apis

import "github.com/gin-gonic/gin"

type Handler interface {
	AuthInfo(c *gin.Context)
	ListNamespaces(c *gin.Context)
	GetClusterSummary(c *gin.Context)
	CreatePipeline(c *gin.Context)
	ListPipelines(c *gin.Context)
	GetPipeline(c *gin.Context)
	UpdatePipeline(c *gin.Context)
	DeletePipeline(c *gin.Context)
	PatchPipeline(c *gin.Context)
	CreateInterStepBufferService(c *gin.Context)
	ListInterStepBufferServices(c *gin.Context)
	GetInterStepBufferService(c *gin.Context)
	UpdateInterStepBufferService(c *gin.Context)
	DeleteInterStepBufferService(c *gin.Context)
	ListPipelineBuffers(c *gin.Context)
	GetPipelineWatermarks(c *gin.Context)
	UpdateVertex(c *gin.Context)
	GetVerticesMetrics(c *gin.Context)
	ListVertexPods(c *gin.Context)
	ListPodsMetrics(c *gin.Context)
	PodLogs(c *gin.Context)
	GetNamespaceEvents(c *gin.Context)
	GetPipelineStatus(c *gin.Context)
}
