package v1

import (
	"sort"
	"strings"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/isbsvc"
)

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
			dto := newPipelineISBStreamDTO(metadata, stream, snapshot.pod.Name, collectedAt(snapshot.jsInfo))
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

func newPipelineISBStreamDTO(metadata pipelineISBResourceMetadata, stream natsserver.StreamDetail, sourcePod, collectedAt string) PipelineISBStreamDTO {
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
		FirstTimestamp:       timestampString(stream.State.FirstTime),
		LastTimestamp:        timestampString(stream.State.LastTime),
		SourcePod:            sourcePod,
		CollectedAt:          collectedAt,
		Scope:                metadata.Scope,
		SharedByInboundEdges: metadata.SharedByInboundEdges,
	}
	if stream.Config != nil {
		dto.Subjects = append([]string{}, stream.Config.Subjects...)
		dto.Storage = strings.ToLower(stream.Config.Storage.String())
		dto.Replicas = stream.Config.Replicas
		dto.Retention = strings.ToLower(stream.Config.Retention.String())
		dto.MaxMessages = stream.Config.MaxMsgs
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
	if isLeaderReported(candidate.SourcePod, candidate.Leader) != isLeaderReported(existing.SourcePod, existing.Leader) {
		return isLeaderReported(candidate.SourcePod, candidate.Leader)
	}
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
			forEachStreamConsumer(stream, func(consumer *natsserver.ConsumerInfo) {
				dto := newPipelineISBConsumerDTO(metadata, stream, consumer, snapshot.pod.Name, collectedAt(snapshot.jsInfo))
				key := dto.Stream + ":" + dto.Consumer
				if existing, ok := consumers[key]; !ok || richerPipelineISBConsumerDTO(dto, existing) {
					consumers[key] = dto
				}
			})
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

func forEachStreamConsumer(stream natsserver.StreamDetail, visit func(*natsserver.ConsumerInfo)) {
	for _, consumer := range stream.Consumer {
		if consumer != nil {
			visit(consumer)
		}
	}
	for _, consumer := range stream.DirectConsumer {
		if consumer != nil {
			visit(consumer)
		}
	}
}

func newPipelineISBConsumerDTO(metadata pipelineISBResourceMetadata, stream natsserver.StreamDetail, consumer *natsserver.ConsumerInfo, sourcePod, collectedAt string) PipelineISBConsumerDTO {
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
		SourcePod:            sourcePod,
		CollectedAt:          collectedAt,
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
	if isLeaderReported(candidate.SourcePod, candidate.Leader) != isLeaderReported(existing.SourcePod, existing.Leader) {
		return isLeaderReported(candidate.SourcePod, candidate.Leader)
	}
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
			dto := newPipelineISBKVStoreDTO(metadata, stream, snapshot.pod.Name, collectedAt(snapshot.jsInfo))
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

func newPipelineISBKVStoreDTO(metadata pipelineISBResourceMetadata, stream natsserver.StreamDetail, sourcePod, collectedAt string) PipelineISBKVStoreDTO {
	values := stream.State.NumSubjects
	if values == 0 && len(stream.State.Subjects) > 0 {
		values = len(stream.State.Subjects)
	}
	if values == 0 && stream.State.Msgs > 0 {
		values = int(stream.State.Msgs)
	}
	dto := PipelineISBKVStoreDTO{
		Namespace:   metadata.Namespace,
		Pipeline:    metadata.Pipeline,
		Scope:       metadata.Scope,
		Direction:   metadata.Direction,
		Vertex:      metadata.Vertex,
		From:        metadata.From,
		To:          metadata.To,
		Bucket:      metadata.KVBucket,
		Stream:      stream.Name,
		Values:      values,
		Bytes:       stream.State.Bytes,
		SourcePod:   sourcePod,
		CollectedAt: collectedAt,
	}
	if stream.Config != nil {
		dto.History = stream.Config.MaxMsgsPer
		dto.TTLSeconds = durationSeconds(stream.Config.MaxAge)
		dto.Replicas = stream.Config.Replicas
		dto.Storage = strings.ToLower(stream.Config.Storage.String())
	}
	if stream.Cluster != nil {
		dto.Leader = stream.Cluster.Leader
	}
	return dto
}

func richerPipelineISBKVStoreDTO(candidate, existing PipelineISBKVStoreDTO) bool {
	if isLeaderReported(candidate.SourcePod, candidate.Leader) != isLeaderReported(existing.SourcePod, existing.Leader) {
		return isLeaderReported(candidate.SourcePod, candidate.Leader)
	}
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

func collectedAt(jsInfo *natsserver.JSInfo) string {
	if jsInfo == nil {
		return ""
	}
	return timestampString(jsInfo.Now)
}

func timestampString(timestamp time.Time) string {
	if timestamp.IsZero() {
		return ""
	}
	return timestamp.Format(time.RFC3339Nano)
}

func isLeaderReported(sourcePod, leader string) bool {
	return sourcePod != "" && sourcePod == leader
}
