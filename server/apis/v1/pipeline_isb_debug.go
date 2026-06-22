package v1

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

type pipelineISBDebugScope struct {
	vertex    string
	from      string
	to        string
	partition *int
}

type pipelineISBDebugContext struct {
	namespace string
	pipeline  *dfv1.Pipeline
	scope     pipelineISBDebugScope
	snapshots []jetStreamMonitorSnapshot
	errors    []ISBMonitorErrorDTO
}

func (h *handler) GetPipelineISBStreams(c *gin.Context) {
	ctx, ok := h.getPipelineISBDebugContext(c)
	if !ok {
		return
	}
	streams := buildPipelineISBStreamDTOs(ctx.snapshots, expectedPipelineISBStreamResources(ctx.pipeline, ctx.scope))
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, PipelineISBStreamsDTO{Streams: streams, Errors: ctx.errors}))
}

func (h *handler) GetPipelineISBConsumers(c *gin.Context) {
	ctx, ok := h.getPipelineISBDebugContext(c)
	if !ok {
		return
	}
	consumers := buildPipelineISBConsumerDTOs(ctx.snapshots, expectedPipelineISBStreamResources(ctx.pipeline, ctx.scope))
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, PipelineISBConsumersDTO{Consumers: consumers, Errors: ctx.errors}))
}

func (h *handler) GetPipelineISBKVStores(c *gin.Context) {
	ctx, ok := h.getPipelineISBDebugContext(c)
	if !ok {
		return
	}
	kvStores := buildPipelineISBKVStoreDTOs(ctx.snapshots, expectedPipelineISBKVResources(ctx.pipeline, ctx.scope))
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, PipelineISBKVStoresDTO{KVStores: kvStores, Errors: ctx.errors}))
}

func (h *handler) getPipelineISBDebugContext(c *gin.Context) (*pipelineISBDebugContext, bool) {
	ns, pipelineName := c.Param("namespace"), c.Param("pipeline")
	pl, err := h.numaflowClient.Pipelines(ns).Get(c, pipelineName, metav1.GetOptions{})
	if err != nil {
		respondWithStatus(c, http.StatusNotFound, fmt.Sprintf("Failed to fetch pipeline %q namespace %q, %s", pipelineName, ns, err.Error()))
		return nil, false
	}
	scope, ok := parsePipelineISBDebugScope(c, pl)
	if !ok {
		return nil, false
	}
	isbsvcName := pl.Spec.InterStepBufferServiceName
	if isbsvcName == "" {
		isbsvcName = dfv1.DefaultISBSvcName
	}
	snapshots, errors, ok := h.collectJetStreamMonitorSnapshots(c, ns, isbsvcName)
	if !ok {
		return nil, false
	}
	if len(snapshots) == 0 && len(errors) > 0 {
		respondWithStatus(c, http.StatusBadGateway, fmt.Sprintf("Failed to fetch JetStream monitor data for pipeline %q namespace %q", pipelineName, ns))
		return nil, false
	}
	return &pipelineISBDebugContext{namespace: ns, pipeline: pl, scope: scope, snapshots: snapshots, errors: errors}, true
}

func parsePipelineISBDebugScope(c *gin.Context, pl *dfv1.Pipeline) (pipelineISBDebugScope, bool) {
	scope := pipelineISBDebugScope{
		vertex: c.Query("vertex"),
		from:   c.Query("from"),
		to:     c.Query("to"),
	}
	if scope.vertex != "" && (scope.from != "" || scope.to != "") {
		respondWithStatus(c, http.StatusBadRequest, "vertex cannot be combined with from/to edge filters")
		return scope, false
	}
	if (scope.from == "") != (scope.to == "") {
		respondWithStatus(c, http.StatusBadRequest, "from and to must be provided together")
		return scope, false
	}
	if partitionValue := c.Query("partition"); partitionValue != "" {
		partition, err := strconv.Atoi(partitionValue)
		if err != nil || partition < 0 {
			respondWithStatus(c, http.StatusBadRequest, fmt.Sprintf("invalid partition %q", partitionValue))
			return scope, false
		}
		scope.partition = &partition
	}
	if scope.vertex != "" {
		vertex := pl.GetVertex(scope.vertex)
		if vertex == nil {
			respondWithStatus(c, http.StatusBadRequest, fmt.Sprintf("vertex %q not found in pipeline %q", scope.vertex, pl.Name))
			return scope, false
		}
		if scope.partition != nil && *scope.partition >= vertex.GetPartitionCount() {
			respondWithStatus(c, http.StatusBadRequest, fmt.Sprintf("partition %d is out of range for vertex %q", *scope.partition, scope.vertex))
			return scope, false
		}
	}
	if scope.from != "" && scope.to != "" {
		if !pipelineHasEdge(pl, scope.from, scope.to) {
			respondWithStatus(c, http.StatusBadRequest, fmt.Sprintf("edge %q -> %q not found in pipeline %q", scope.from, scope.to, pl.Name))
			return scope, false
		}
		toVertex := pl.GetVertex(scope.to)
		if toVertex == nil {
			respondWithStatus(c, http.StatusBadRequest, fmt.Sprintf("vertex %q not found in pipeline %q", scope.to, pl.Name))
			return scope, false
		}
		if scope.partition != nil && *scope.partition >= toVertex.GetPartitionCount() {
			respondWithStatus(c, http.StatusBadRequest, fmt.Sprintf("partition %d is out of range for edge %q -> %q", *scope.partition, scope.from, scope.to))
			return scope, false
		}
	}
	return scope, true
}

func pipelineHasEdge(pl *dfv1.Pipeline, from, to string) bool {
	for _, edge := range pl.ListAllEdges() {
		if edge.From == from && edge.To == to {
			return true
		}
	}
	return false
}

func respondWithStatus(c *gin.Context, status int, message string) {
	c.JSON(status, NewNumaflowAPIResponse(&message, nil))
}
