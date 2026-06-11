package v1

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	natsserver "github.com/nats-io/nats-server/v2/server"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
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

type pipelineISBResourceMetadata struct {
	Namespace            string
	Pipeline             string
	Vertex               string
	From                 string
	To                   string
	Partition            int
	Scope                string
	Direction            string
	Bucket               string
	KVBucket             string
	Stream               string
	SharedByInboundEdges bool
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

func expectedPipelineISBStreamResources(pl *dfv1.Pipeline, scope pipelineISBDebugScope) map[string]pipelineISBResourceMetadata {
	resources := map[string]pipelineISBResourceMetadata{}
	if scope.vertex != "" {
		addExpectedStreamResources(resources, pl, scope.vertex, "vertex", "", "", scope)
		return resources
	}
	if scope.from != "" && scope.to != "" {
		addExpectedStreamResources(resources, pl, scope.to, "targetVertex", scope.from, scope.to, scope)
		return resources
	}
	for _, vertex := range pl.Spec.Vertices {
		addExpectedStreamResources(resources, pl, vertex.Name, "vertex", "", "", scope)
	}
	return resources
}

func addExpectedStreamResources(resources map[string]pipelineISBResourceMetadata, pl *dfv1.Pipeline, vertexName, scopeName, from, to string, scope pipelineISBDebugScope) {
	vertex := pl.GetVertex(vertexName)
	if vertex == nil || vertex.IsASource() {
		return
	}
	for partition := 0; partition < vertex.GetPartitionCount(); partition++ {
		if scope.partition != nil && *scope.partition != partition {
			continue
		}
		streamName := dfv1.GenerateBufferName(pl.Namespace, pl.Name, vertexName, partition)
		resources[streamName] = pipelineISBResourceMetadata{
			Namespace:            pl.Namespace,
			Pipeline:             pl.Name,
			Vertex:               vertexName,
			From:                 from,
			To:                   to,
			Partition:            partition,
			Scope:                scopeName,
			Stream:               streamName,
			SharedByInboundEdges: from != "" && len(pl.GetFromEdges(vertexName)) > 1,
		}
	}
}

func expectedPipelineISBKVResources(pl *dfv1.Pipeline, scope pipelineISBDebugScope) map[string]pipelineISBResourceMetadata {
	resources := map[string]pipelineISBResourceMetadata{}
	if scope.vertex != "" {
		vertex := pl.GetVertex(scope.vertex)
		if vertex == nil {
			return resources
		}
		if vertex.IsASource() {
			addExpectedKVResource(resources, pl, dfv1.GenerateSourceBucketName(pl.Namespace, pl.Name, vertex.Name), "source", "read", vertex.Name, "", "")
		}
		if vertex.IsASink() {
			addExpectedKVResource(resources, pl, dfv1.GenerateSinkBucketName(pl.Namespace, pl.Name, vertex.Name), "sink", "write", vertex.Name, "", "")
		}
		for _, edge := range pl.GetFromEdges(vertex.Name) {
			addExpectedKVResource(resources, pl, dfv1.GenerateEdgeBucketName(pl.Namespace, pl.Name, edge.From, edge.To), "edge", "read", vertex.Name, edge.From, edge.To)
		}
		for _, edge := range pl.GetToEdges(vertex.Name) {
			addExpectedKVResource(resources, pl, dfv1.GenerateEdgeBucketName(pl.Namespace, pl.Name, edge.From, edge.To), "edge", "write", vertex.Name, edge.From, edge.To)
		}
		return resources
	}
	if scope.from != "" && scope.to != "" {
		addExpectedKVResource(resources, pl, dfv1.GenerateEdgeBucketName(pl.Namespace, pl.Name, scope.from, scope.to), "edge", "", "", scope.from, scope.to)
		return resources
	}
	for _, edge := range pl.ListAllEdges() {
		addExpectedKVResource(resources, pl, dfv1.GenerateEdgeBucketName(pl.Namespace, pl.Name, edge.From, edge.To), "edge", "", "", edge.From, edge.To)
	}
	for _, vertex := range pl.Spec.Vertices {
		if vertex.IsASource() {
			addExpectedKVResource(resources, pl, dfv1.GenerateSourceBucketName(pl.Namespace, pl.Name, vertex.Name), "source", "", vertex.Name, "", "")
		}
		if vertex.IsASink() {
			addExpectedKVResource(resources, pl, dfv1.GenerateSinkBucketName(pl.Namespace, pl.Name, vertex.Name), "sink", "", vertex.Name, "", "")
		}
	}
	return resources
}

func addExpectedKVResource(resources map[string]pipelineISBResourceMetadata, pl *dfv1.Pipeline, bucket, scope, direction, vertex, from, to string) {
	kvBucket := isbsvc.JetStreamOTKVName(bucket)
	streamName := "KV_" + kvBucket
	metadata := pipelineISBResourceMetadata{
		Namespace: pl.Namespace,
		Pipeline:  pl.Name,
		Vertex:    vertex,
		From:      from,
		To:        to,
		Scope:     scope,
		Direction: direction,
		Bucket:    bucket,
		KVBucket:  kvBucket,
		Stream:    streamName,
	}
	if existing, ok := resources[streamName]; ok {
		if existing.Direction != "" && direction != "" && existing.Direction != direction {
			existing.Direction = "read/write"
		}
		resources[streamName] = existing
		return
	}
	resources[streamName] = metadata
}

func buildPipelineISBStreamDTOs(snapshots []jetStreamMonitorSnapshot, expected map[string]pipelineISBResourceMetadata) []PipelineISBStreamDTO {
	streams := map[string]PipelineISBStreamDTO{}
	for _, snapshot := range snapshots {
		forEachJetStreamDetail(snapshot.jsInfo, func(stream natsserver.StreamDetail) {
			metadata, ok := expected[stream.Name]
			if !ok {
				return
			}
			dto := newPipelineISBStreamDTO(metadata, stream)
			if existing, ok := streams[stream.Name]; !ok || richerPipelineISBStreamDTO(dto, existing) {
				streams[stream.Name] = dto
			}
		})
	}
	result := make([]PipelineISBStreamDTO, 0, len(streams))
	for _, stream := range streams {
		result = append(result, stream)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Vertex != result[j].Vertex {
			return result[i].Vertex < result[j].Vertex
		}
		if result[i].Partition != result[j].Partition {
			return result[i].Partition < result[j].Partition
		}
		return result[i].Stream < result[j].Stream
	})
	return result
}

func newPipelineISBStreamDTO(metadata pipelineISBResourceMetadata, stream natsserver.StreamDetail) PipelineISBStreamDTO {
	dto := PipelineISBStreamDTO{
		Namespace:            metadata.Namespace,
		Pipeline:             metadata.Pipeline,
		Vertex:               metadata.Vertex,
		From:                 metadata.From,
		To:                   metadata.To,
		Partition:            metadata.Partition,
		Stream:               stream.Name,
		Messages:             stream.State.Msgs,
		Bytes:                stream.State.Bytes,
		ConsumerCount:        stream.State.Consumers,
		FirstSeq:             stream.State.FirstSeq,
		LastSeq:              stream.State.LastSeq,
		Scope:                metadata.Scope,
		SharedByInboundEdges: metadata.SharedByInboundEdges,
	}
	if stream.Config != nil {
		dto.Subjects = append([]string{}, stream.Config.Subjects...)
		dto.Storage = strings.ToLower(stream.Config.Storage.String())
		dto.Replicas = stream.Config.Replicas
		dto.Retention = strings.ToLower(stream.Config.Retention.String())
	}
	if len(dto.Subjects) == 0 {
		dto.Subjects = []string{stream.Name}
	}
	if stream.Cluster != nil {
		dto.Leader = stream.Cluster.Leader
	}
	return dto
}

func richerPipelineISBStreamDTO(candidate, existing PipelineISBStreamDTO) bool {
	if existing.Leader == "" && candidate.Leader != "" {
		return true
	}
	if existing.Storage == "" && candidate.Storage != "" {
		return true
	}
	return existing.ConsumerCount == 0 && candidate.ConsumerCount > 0
}

func buildPipelineISBConsumerDTOs(snapshots []jetStreamMonitorSnapshot, expected map[string]pipelineISBResourceMetadata) []PipelineISBConsumerDTO {
	consumers := map[string]PipelineISBConsumerDTO{}
	for _, snapshot := range snapshots {
		forEachJetStreamDetail(snapshot.jsInfo, func(stream natsserver.StreamDetail) {
			metadata, ok := expected[stream.Name]
			if !ok {
				return
			}
			for _, consumer := range append(stream.Consumer, stream.DirectConsumer...) {
				if consumer == nil {
					continue
				}
				dto := newPipelineISBConsumerDTO(metadata, stream, consumer)
				key := dto.Stream + ":" + dto.Consumer
				if existing, ok := consumers[key]; !ok || richerPipelineISBConsumerDTO(dto, existing) {
					consumers[key] = dto
				}
			}
		})
	}
	result := make([]PipelineISBConsumerDTO, 0, len(consumers))
	for _, consumer := range consumers {
		result = append(result, consumer)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Vertex != result[j].Vertex {
			return result[i].Vertex < result[j].Vertex
		}
		if result[i].Partition != result[j].Partition {
			return result[i].Partition < result[j].Partition
		}
		if result[i].Stream != result[j].Stream {
			return result[i].Stream < result[j].Stream
		}
		return result[i].Consumer < result[j].Consumer
	})
	return result
}

func newPipelineISBConsumerDTO(metadata pipelineISBResourceMetadata, stream natsserver.StreamDetail, consumer *natsserver.ConsumerInfo) PipelineISBConsumerDTO {
	consumerName := consumer.Name
	streamName := consumer.Stream
	if streamName == "" {
		streamName = stream.Name
	}
	dto := PipelineISBConsumerDTO{
		Namespace:            metadata.Namespace,
		Pipeline:             metadata.Pipeline,
		Vertex:               metadata.Vertex,
		From:                 metadata.From,
		To:                   metadata.To,
		Partition:            metadata.Partition,
		Stream:               streamName,
		Consumer:             consumerName,
		NumAckPending:        consumer.NumAckPending,
		NumRedelivered:       consumer.NumRedelivered,
		NumWaiting:           consumer.NumWaiting,
		NumPending:           consumer.NumPending,
		DeliveredConsumerSeq: consumer.Delivered.Consumer,
		DeliveredStreamSeq:   consumer.Delivered.Stream,
		AckFloorConsumerSeq:  consumer.AckFloor.Consumer,
		AckFloorStreamSeq:    consumer.AckFloor.Stream,
		Scope:                metadata.Scope,
		SharedByInboundEdges: metadata.SharedByInboundEdges,
	}
	if consumer.Config != nil {
		if consumerName == "" {
			consumerName = consumer.Config.Name
		}
		if consumerName == "" {
			consumerName = consumer.Config.Durable
		}
		dto.Consumer = consumerName
		dto.Durable = consumer.Config.Durable
		dto.FilterSubject = consumer.Config.FilterSubject
		dto.FilterSubjects = append([]string{}, consumer.Config.FilterSubjects...)
		dto.AckPolicy = strings.ToLower(consumer.Config.AckPolicy.String())
		dto.DeliverPolicy = strings.ToLower(consumer.Config.DeliverPolicy.String())
		dto.AckWaitSeconds = durationSeconds(consumer.Config.AckWait)
		dto.MaxAckPending = consumer.Config.MaxAckPending
	}
	if consumer.Cluster != nil {
		dto.Leader = consumer.Cluster.Leader
	} else if stream.Cluster != nil {
		dto.Leader = stream.Cluster.Leader
	}
	return dto
}

func richerPipelineISBConsumerDTO(candidate, existing PipelineISBConsumerDTO) bool {
	if existing.Leader == "" && candidate.Leader != "" {
		return true
	}
	if existing.AckPolicy == "" && candidate.AckPolicy != "" {
		return true
	}
	return existing.Consumer == "" && candidate.Consumer != ""
}

func buildPipelineISBKVStoreDTOs(snapshots []jetStreamMonitorSnapshot, expected map[string]pipelineISBResourceMetadata) []PipelineISBKVStoreDTO {
	kvStores := map[string]PipelineISBKVStoreDTO{}
	for _, snapshot := range snapshots {
		forEachJetStreamDetail(snapshot.jsInfo, func(stream natsserver.StreamDetail) {
			metadata, ok := expected[stream.Name]
			if !ok {
				return
			}
			dto := newPipelineISBKVStoreDTO(metadata, stream)
			if existing, ok := kvStores[stream.Name]; !ok || richerPipelineISBKVStoreDTO(dto, existing) {
				kvStores[stream.Name] = dto
			}
		})
	}
	result := make([]PipelineISBKVStoreDTO, 0, len(kvStores))
	for _, kvStore := range kvStores {
		result = append(result, kvStore)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Scope != result[j].Scope {
			return result[i].Scope < result[j].Scope
		}
		if result[i].Vertex != result[j].Vertex {
			return result[i].Vertex < result[j].Vertex
		}
		if pipelineISBKVDirectionRank(result[i].Direction) != pipelineISBKVDirectionRank(result[j].Direction) {
			return pipelineISBKVDirectionRank(result[i].Direction) < pipelineISBKVDirectionRank(result[j].Direction)
		}
		if result[i].From != result[j].From {
			return result[i].From < result[j].From
		}
		if result[i].To != result[j].To {
			return result[i].To < result[j].To
		}
		return result[i].Bucket < result[j].Bucket
	})
	return result
}

func pipelineISBKVDirectionRank(direction string) int {
	switch direction {
	case "read":
		return 0
	case "write":
		return 1
	case "read/write":
		return 2
	default:
		return 3
	}
}

func newPipelineISBKVStoreDTO(metadata pipelineISBResourceMetadata, stream natsserver.StreamDetail) PipelineISBKVStoreDTO {
	values := stream.State.NumSubjects
	if values == 0 && len(stream.State.Subjects) > 0 {
		values = len(stream.State.Subjects)
	}
	if values == 0 && stream.State.Msgs > 0 {
		values = int(stream.State.Msgs)
	}
	dto := PipelineISBKVStoreDTO{
		Namespace: metadata.Namespace,
		Pipeline:  metadata.Pipeline,
		Scope:     metadata.Scope,
		Direction: metadata.Direction,
		Vertex:    metadata.Vertex,
		From:      metadata.From,
		To:        metadata.To,
		Bucket:    metadata.KVBucket,
		Stream:    stream.Name,
		Values:    values,
		Bytes:     stream.State.Bytes,
	}
	if stream.Config != nil {
		dto.History = stream.Config.MaxMsgsPer
		dto.TTLSeconds = durationSeconds(stream.Config.MaxAge)
		dto.Replicas = stream.Config.Replicas
		dto.Storage = strings.ToLower(stream.Config.Storage.String())
	}
	return dto
}

func richerPipelineISBKVStoreDTO(candidate, existing PipelineISBKVStoreDTO) bool {
	if existing.Storage == "" && candidate.Storage != "" {
		return true
	}
	return existing.Values == 0 && candidate.Values > 0
}

func forEachJetStreamDetail(jsInfo *natsserver.JSInfo, visit func(natsserver.StreamDetail)) {
	if jsInfo == nil {
		return
	}
	for _, account := range jsInfo.AccountDetails {
		if account == nil {
			continue
		}
		for _, stream := range account.Streams {
			visit(stream)
		}
	}
}

func durationSeconds(duration time.Duration) float64 {
	if duration == 0 {
		return 0
	}
	return duration.Seconds()
}
