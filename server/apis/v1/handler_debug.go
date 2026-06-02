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

package v1

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// GetPipelineTopology returns the pipeline's graph (vertices with roles,
// partitions, scale bounds, expected containers, and edges), derived from spec.
func (h *handler) GetPipelineTopology(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	pl, err := h.numaflowClient.Pipelines(ns).Get(c, pipeline, metav1.GetOptions{})
	if err != nil {
		h.respondWithStatusError(c, http.StatusNotFound, fmt.Sprintf("failed to fetch pipeline %q namespace %q: %v", pipeline, ns, err))
		return
	}
	c.JSON(http.StatusOK, newPipelineTopologyDTO(pl))
}

// GetPipelineBuffers returns all inter-step buffers of a pipeline as typed DTOs.
func (h *handler) GetPipelineBuffers(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	client, err := h.getPipelineDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithStatusError(c, http.StatusBadGateway, fmt.Sprintf("failed to get daemon service client for pipeline %q: %v", pipeline, err))
		return
	}
	buffers, err := client.ListPipelineBuffers(c, pipeline)
	if err != nil {
		h.respondWithStatusError(c, http.StatusBadGateway, fmt.Sprintf("failed to list buffers for pipeline %q: %v", pipeline, err))
		return
	}
	out := make([]BufferDTO, 0, len(buffers))
	for _, b := range buffers {
		if b != nil {
			out = append(out, newBufferDTO(b))
		}
	}
	c.JSON(http.StatusOK, out)
}

// GetPipelineEdgeWatermarks returns the per-edge watermarks of a pipeline as typed DTOs.
func (h *handler) GetPipelineEdgeWatermarks(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	client, err := h.getPipelineDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithStatusError(c, http.StatusBadGateway, fmt.Sprintf("failed to get daemon service client for pipeline %q: %v", pipeline, err))
		return
	}
	watermarks, err := client.GetPipelineWatermarks(c, pipeline)
	if err != nil {
		h.respondWithStatusError(c, http.StatusBadGateway, fmt.Sprintf("failed to get watermarks for pipeline %q: %v", pipeline, err))
		return
	}
	c.JSON(http.StatusOK, newEdgeWatermarkDTOs(watermarks))
}

// GetMonoVertexThroughputMetrics returns the processing rates and pending counts
// of a mono vertex as a typed DTO.
func (h *handler) GetMonoVertexThroughputMetrics(c *gin.Context) {
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")

	client, err := h.getMonoVertexDaemonClient(ns, monoVertex)
	if err != nil || client == nil {
		h.respondWithStatusError(c, http.StatusBadGateway, fmt.Sprintf("failed to get daemon service client for mono vertex %q: %v", monoVertex, err))
		return
	}
	metrics, err := client.GetMonoVertexMetrics(c)
	if err != nil {
		h.respondWithStatusError(c, http.StatusBadGateway, fmt.Sprintf("failed to get the metrics for mono vertex %q: %v", monoVertex, err))
		return
	}
	c.JSON(http.StatusOK, newMonoVertexMetricsDTO(monoVertex, metrics))
}

// defaultScopedEventLimit is the number of most-recent events returned by the
// scoped/snapshot event endpoints when the caller does not specify a limit.
const defaultScopedEventLimit int64 = 100

// eventListPageSize is the per-page size used while paging the field-selected
// event list; eventScanCeiling bounds the total events scanned so a pathological
// namespace cannot cause an unbounded fetch.
const (
	eventListPageSize int64 = 500
	eventScanCeiling  int   = 5000
)

// eventFilter narrows an event list query.
type eventFilter struct {
	objectKind string // involvedObject.kind
	objectName string // involvedObject.name
	eventType  string // "Warning" | "Normal" | ""
	limit      int64  // 0 => defaultScopedEventLimit
}

type eventListMetadata struct {
	limited            bool
	scanCeilingReached bool
}

func (m eventListMetadata) truncated() bool {
	return m.limited || m.scanCeilingReached
}

func setEventListHeaders(c *gin.Context, meta eventListMetadata) {
	if !meta.truncated() {
		return
	}
	c.Header("X-Numaflow-Events-Truncated", "true")
	if meta.scanCeilingReached {
		c.Header("X-Numaflow-Events-Scan-Ceiling-Reached", "true")
	}
}

func vertexCRName(pipeline, vertex string) string {
	return fmt.Sprintf("%s-%s", pipeline, vertex)
}

// listScopedEvents lists namespace events narrowed by the given filter. The
// filter is pushed into the Kubernetes API call via a field selector (so noisy
// namespaces don't load every event), with an in-memory pass as a backstop
// (field selectors are best-effort and the fake client in tests ignores them).
// It is the shared core used by GetNamespaceEvents and the scoped-event routes.
func (h *handler) listScopedEvents(ctx context.Context, ns string, f eventFilter) ([]K8sEventsResponse, eventListMetadata, error) {
	selectors := fields.Set{}
	if f.objectKind != "" {
		selectors["involvedObject.kind"] = f.objectKind
	}
	if f.objectName != "" {
		selectors["involvedObject.name"] = f.objectName
	}
	if f.eventType != "" {
		selectors["type"] = f.eventType
	}
	limit := f.limit
	if limit <= 0 {
		limit = defaultScopedEventLimit
	}

	// The field selector narrows the result set server-side, but Kubernetes list
	// pagination is not ordered by timestamp, so we cannot use ListOptions.Limit
	// to mean "most recent N". Instead we page through the filtered events
	// (bounded by a hard ceiling), sort by timestamp in memory, then cap to limit.
	fieldSelector := ""
	if len(selectors) > 0 {
		fieldSelector = selectors.AsSelector().String()
	}

	var (
		response          []K8sEventsResponse
		defaultTimeObject time.Time
		continueToken     string
		meta              eventListMetadata
		scanned           int
	)
	for {
		events, err := h.kubeClient.CoreV1().Events(ns).List(ctx, metav1.ListOptions{
			FieldSelector: fieldSelector,
			Limit:         eventListPageSize,
			Continue:      continueToken,
		})
		if err != nil {
			return nil, eventListMetadata{}, err
		}
		scanned += len(events.Items)
		for _, event := range events.Items {
			if event.LastTimestamp.Time.Equal(defaultTimeObject) {
				continue
			}
			// Backstop the field selector in memory (no-op when the server already filtered).
			if f.objectKind != "" && !strings.EqualFold(event.InvolvedObject.Kind, f.objectKind) {
				continue
			}
			if f.objectName != "" && !strings.EqualFold(event.InvolvedObject.Name, f.objectName) {
				continue
			}
			if f.eventType != "" && !strings.EqualFold(event.Type, f.eventType) {
				continue
			}
			response = append(response, NewK8sEventsResponse(event.LastTimestamp.UnixMilli(), event.Type,
				event.InvolvedObject.Kind, event.InvolvedObject.Name, event.Reason, event.Message, event.Count))
		}
		continueToken = events.Continue
		// Stop when there are no more pages or we've scanned the safety ceiling.
		if continueToken == "" {
			break
		}
		if scanned >= eventScanCeiling {
			meta.scanCeilingReached = true
			break
		}
	}

	// Most-recent-first, then cap to the requested limit.
	sort.Slice(response, func(i int, j int) bool {
		return response[i].TimeStamp >= response[j].TimeStamp
	})
	if int64(len(response)) > limit {
		meta.limited = true
		response = response[:limit]
	}
	return response, meta, nil
}

// GetPipelineEvents returns Kubernetes events for a pipeline object.
func (h *handler) GetPipelineEvents(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	events, meta, err := h.listScopedEvents(c, ns, eventFilter{objectKind: dfv1.PipelineGroupVersionKind.Kind, objectName: pipeline, limit: limit})
	if err != nil {
		h.respondWithStatusError(c, http.StatusBadGateway, fmt.Sprintf("failed to get events for pipeline %q: %v", pipeline, err))
		return
	}
	setEventListHeaders(c, meta)
	c.JSON(http.StatusOK, events)
}

// GetVertexEvents returns Kubernetes events for a vertex. K8s events reference
// the generated Vertex CR name (<pipeline>-<vertex>), so we resolve that here.
func (h *handler) GetVertexEvents(c *gin.Context) {
	ns, pipeline, vertex := c.Param("namespace"), c.Param("pipeline"), c.Param("vertex")
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	events, meta, err := h.listScopedEvents(c, ns, eventFilter{objectKind: dfv1.VertexGroupVersionKind.Kind, objectName: vertexCRName(pipeline, vertex), limit: limit})
	if err != nil {
		h.respondWithStatusError(c, http.StatusBadGateway, fmt.Sprintf("failed to get events for pipeline %q vertex %q: %v", pipeline, vertex, err))
		return
	}
	setEventListHeaders(c, meta)
	c.JSON(http.StatusOK, events)
}

// GetMonoVertexEvents returns Kubernetes events for a mono vertex object.
func (h *handler) GetMonoVertexEvents(c *gin.Context) {
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	events, meta, err := h.listScopedEvents(c, ns, eventFilter{objectKind: dfv1.MonoVertexGroupVersionKind.Kind, objectName: monoVertex, limit: limit})
	if err != nil {
		h.respondWithStatusError(c, http.StatusBadGateway, fmt.Sprintf("failed to get events for mono vertex %q: %v", monoVertex, err))
		return
	}
	setEventListHeaders(c, meta)
	c.JSON(http.StatusOK, events)
}

// Caps that bound the debug-snapshot fan-out so it can never become the heaviest API.
const (
	snapshotMaxVertices      = 50
	snapshotMaxErrorsPerVtx  = 20
	snapshotMaxTotalErrors   = 200
	snapshotMaxWarningEvents = 50
	snapshotOverallTimeout   = 30 * time.Second
	snapshotSubCallTimeout   = 3 * time.Second
)

// snapshotSubCallContext derives a fresh per-sub-call timeout context from the request,
// so one slow daemon call cannot starve later sections of the snapshot.
func snapshotSubCallContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, snapshotSubCallTimeout)
}

// newSnapshotDaemonError classifies a failed daemon sub-call: a deadline gets the
// stable timeout code, everything else daemon_unavailable.
func newSnapshotDaemonError(err error) *SnapshotSectionError {
	if errors.Is(err, context.DeadlineExceeded) {
		return &SnapshotSectionError{Code: SnapshotErrCodeTimeout, Message: err.Error()}
	}
	return &SnapshotSectionError{Code: SnapshotErrCodeDaemonUnavailable, Message: err.Error()}
}

func capReplicaErrors(replicas []ReplicaErrorDTO, remainingErrors *int) []ReplicaErrorDTO {
	kept := make([]ReplicaErrorDTO, 0, len(replicas))
	for i := range replicas {
		if *remainingErrors <= 0 {
			break
		}
		limit := snapshotMaxErrorsPerVtx
		if limit > *remainingErrors {
			limit = *remainingErrors
		}
		if len(replicas[i].Errors) > limit {
			replicas[i].Errors = replicas[i].Errors[:limit]
		}
		*remainingErrors -= len(replicas[i].Errors)
		kept = append(kept, replicas[i])
	}
	return kept
}

// GetPipelineDebugSnapshot returns a bounded, read-only correlation of a
// pipeline's current state. Each section is fetched independently; a failed
// section carries a structured error instead of failing the whole response.
func (h *handler) GetPipelineDebugSnapshot(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	pl, err := h.numaflowClient.Pipelines(ns).Get(c, pipeline, metav1.GetOptions{})
	if err != nil {
		h.respondWithStatusError(c, http.StatusNotFound, fmt.Sprintf("failed to fetch pipeline %q namespace %q: %v", pipeline, ns, err))
		return
	}

	now := func() string { return time.Now().UTC().Format(time.RFC3339) }
	snap := DebugSnapshotDTO{Pipeline: pipeline, Namespace: ns, ObservedAt: now()}
	snapshotCtx, cancelSnapshot := context.WithTimeout(c.Request.Context(), snapshotOverallTimeout)
	defer cancelSnapshot()

	// Resource health (K8s-side, cached).
	snap.ResourceHealth.ObservedAt = now()
	if rh, rerr := h.healthChecker.getPipelineResourceHealth(h, ns, pipeline); rerr != nil {
		snap.ResourceHealth.Error = &SnapshotSectionError{Code: SnapshotErrCodeInternal, Message: rerr.Error()}
	} else {
		snap.ResourceHealth.Data = &ResourceHealthData{Status: rh.Status, Message: rh.Message, Code: rh.Code}
	}

	// All remaining sections need the daemon client.
	client, derr := h.getPipelineDaemonClient(ns, pipeline)
	if derr != nil || client == nil {
		se := &SnapshotSectionError{Code: SnapshotErrCodeDaemonUnavailable, Message: fmt.Sprintf("daemon unavailable: %v", derr)}
		snap.DataHealth.Error, snap.WatermarkLag.Error = se, se
		snap.Vertices.Error, snap.Buffers.Error, snap.RuntimeErrors.Error = se, se, se
		snap.DataHealth.ObservedAt, snap.WatermarkLag.ObservedAt = now(), now()
		snap.Vertices.ObservedAt, snap.Buffers.ObservedAt, snap.RuntimeErrors.ObservedAt = now(), now(), now()
	} else {
		// Data health (own timeout).
		snap.DataHealth.ObservedAt = now()
		func() {
			dctx, cancel := snapshotSubCallContext(snapshotCtx)
			defer cancel()
			if st, e := client.GetPipelineStatus(dctx, pipeline); e != nil {
				snap.DataHealth.Error = newSnapshotDaemonError(e)
			} else {
				snap.DataHealth.Data = &DataHealthDTO{Status: st.GetStatus(), Message: st.GetMessage(), Code: st.GetCode()}
			}
		}()

		// Watermark lag (own timeout).
		snap.WatermarkLag.ObservedAt = now()
		func() {
			dctx, cancel := snapshotSubCallContext(snapshotCtx)
			defer cancel()
			if wms, e := client.GetPipelineWatermarks(dctx, pipeline); e != nil {
				snap.WatermarkLag.Error = newSnapshotDaemonError(e)
			} else {
				lag := computePipelineWatermarkLag(pl, wms)
				snap.WatermarkLag.Data = &lag
			}
		}()

		// Buffers (own timeout).
		snap.Buffers.ObservedAt = now()
		func() {
			dctx, cancel := snapshotSubCallContext(snapshotCtx)
			defer cancel()
			if buffers, e := client.ListPipelineBuffers(dctx, pipeline); e != nil {
				snap.Buffers.Error = newSnapshotDaemonError(e)
			} else {
				for _, b := range buffers {
					if b != nil {
						snap.Buffers.Data = append(snap.Buffers.Data, newBufferDTO(b))
					}
				}
			}
		}()

		// Per-vertex metrics and runtime errors (bounded).
		snap.Vertices.ObservedAt = now()
		snap.RuntimeErrors.ObservedAt = now()
		vertices := pl.Spec.Vertices
		if len(vertices) > snapshotMaxVertices {
			vertices = vertices[:snapshotMaxVertices]
			snap.Notes = append(snap.Notes, fmt.Sprintf("vertices truncated to %d", snapshotMaxVertices))
		}
		remainingErrors := snapshotMaxTotalErrors
		for _, v := range vertices {
			if errors.Is(snapshotCtx.Err(), context.DeadlineExceeded) {
				timeoutErr := newSnapshotDaemonError(snapshotCtx.Err())
				if snap.Vertices.Error == nil {
					snap.Vertices.Error = timeoutErr
				}
				if snap.RuntimeErrors.Error == nil {
					snap.RuntimeErrors.Error = timeoutErr
				}
				snap.Notes = append(snap.Notes, fmt.Sprintf("debug snapshot exceeded %s overall timeout", snapshotOverallTimeout))
				break
			}
			func() {
				dctx, cancel := snapshotSubCallContext(snapshotCtx)
				defer cancel()
				if vm, e := client.GetVertexMetrics(dctx, pipeline, v.Name); e == nil {
					snap.Vertices.Data = append(snap.Vertices.Data, SnapshotVertexMetrics{
						Vertex:     v.Name,
						Partitions: newVertexMetricsDTO(pipeline, v.Name, vm).Partitions,
					})
				} else if snap.Vertices.Error == nil {
					snap.Vertices.Error = newSnapshotDaemonError(e)
				}
			}()

			if remainingErrors <= 0 {
				snap.Notes = append(snap.Notes, fmt.Sprintf("runtime errors truncated at %d total", snapshotMaxTotalErrors))
				break
			}
			func() {
				dctx, cancel := snapshotSubCallContext(snapshotCtx)
				defer cancel()
				errs, e := client.GetVertexErrors(dctx, pipeline, v.Name)
				if e != nil {
					if snap.RuntimeErrors.Error == nil {
						snap.RuntimeErrors.Error = newSnapshotDaemonError(e)
					}
					return
				}
				replicas := newReplicaErrorDTOs(errs)
				var kept []ReplicaErrorDTO
				for i := range replicas {
					if remainingErrors <= 0 {
						break
					}
					// Bound each replica by both the per-replica cap and the
					// remaining total budget.
					limit := snapshotMaxErrorsPerVtx
					if limit > remainingErrors {
						limit = remainingErrors
					}
					if len(replicas[i].Errors) > limit {
						replicas[i].Errors = replicas[i].Errors[:limit]
					}
					remainingErrors -= len(replicas[i].Errors)
					kept = append(kept, replicas[i])
				}
				if len(kept) > 0 {
					snap.RuntimeErrors.Data = append(snap.RuntimeErrors.Data, SnapshotVertexErrors{Vertex: v.Name, Errors: kept})
				}
			}()
		}
	}

	// Recent Warning events for the pipeline (bounded server-side by type+limit).
	snap.WarningEvents.ObservedAt = now()
	warnFilter := eventFilter{
		objectKind: dfv1.PipelineGroupVersionKind.Kind,
		objectName: pipeline,
		eventType:  "Warning",
		limit:      snapshotMaxWarningEvents,
	}
	if events, meta, e := h.listScopedEvents(snapshotCtx, ns, warnFilter); e != nil {
		snap.WarningEvents.Error = &SnapshotSectionError{Code: SnapshotErrCodeInternal, Message: e.Error()}
	} else {
		if meta.limited {
			snap.Notes = append(snap.Notes, fmt.Sprintf("warning events truncated at %d", snapshotMaxWarningEvents))
		}
		if meta.scanCeilingReached {
			snap.Notes = append(snap.Notes, fmt.Sprintf("warning event scan reached %d event ceiling; most recent warning events may be incomplete", eventScanCeiling))
		}
		snap.WarningEvents.Data = events
	}

	c.JSON(http.StatusOK, snap)
}

// GetMonoVertexDebugSnapshot returns a bounded, read-only correlation of a mono
// vertex's current state. Each section is fetched independently; a failed
// section carries a structured error instead of failing the whole response.
func (h *handler) GetMonoVertexDebugSnapshot(c *gin.Context) {
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")

	if _, err := h.numaflowClient.MonoVertices(ns).Get(c, monoVertex, metav1.GetOptions{}); err != nil {
		h.respondWithStatusError(c, http.StatusNotFound, fmt.Sprintf("failed to fetch mono vertex %q namespace %q: %v", monoVertex, ns, err))
		return
	}

	now := func() string { return time.Now().UTC().Format(time.RFC3339) }
	snap := MonoVertexDebugSnapshotDTO{MonoVertex: monoVertex, Namespace: ns, ObservedAt: now()}
	snapshotCtx, cancelSnapshot := context.WithTimeout(c.Request.Context(), snapshotOverallTimeout)
	defer cancelSnapshot()

	snap.ResourceHealth.ObservedAt = now()
	if rh, rerr := h.healthChecker.getMonoVtxResourceHealth(h, ns, monoVertex); rerr != nil {
		snap.ResourceHealth.Error = &SnapshotSectionError{Code: SnapshotErrCodeInternal, Message: rerr.Error()}
	} else {
		snap.ResourceHealth.Data = &ResourceHealthData{Status: rh.Status, Message: rh.Message, Code: rh.Code}
	}

	client, derr := h.getMonoVertexDaemonClient(ns, monoVertex)
	if derr != nil || client == nil {
		se := &SnapshotSectionError{Code: SnapshotErrCodeDaemonUnavailable, Message: fmt.Sprintf("daemon unavailable: %v", derr)}
		snap.DataHealth.Error, snap.Metrics.Error, snap.RuntimeErrors.Error = se, se, se
		snap.DataHealth.ObservedAt, snap.Metrics.ObservedAt, snap.RuntimeErrors.ObservedAt = now(), now(), now()
	} else {
		snap.DataHealth.ObservedAt = now()
		func() {
			dctx, cancel := snapshotSubCallContext(snapshotCtx)
			defer cancel()
			if st, e := client.GetMonoVertexStatus(dctx); e != nil {
				snap.DataHealth.Error = newSnapshotDaemonError(e)
			} else if st == nil {
				snap.DataHealth.Error = &SnapshotSectionError{Code: SnapshotErrCodeInternal, Message: "mono vertex daemon returned nil status"}
			} else {
				snap.DataHealth.Data = &DataHealthDTO{Status: st.GetStatus(), Message: st.GetMessage(), Code: st.GetCode()}
			}
		}()

		snap.Metrics.ObservedAt = now()
		func() {
			dctx, cancel := snapshotSubCallContext(snapshotCtx)
			defer cancel()
			if metrics, e := client.GetMonoVertexMetrics(dctx); e != nil {
				snap.Metrics.Error = newSnapshotDaemonError(e)
			} else {
				dto := newMonoVertexMetricsDTO(monoVertex, metrics)
				snap.Metrics.Data = &dto
			}
		}()

		snap.RuntimeErrors.ObservedAt = now()
		func() {
			dctx, cancel := snapshotSubCallContext(snapshotCtx)
			defer cancel()
			errs, e := client.GetMonoVertexErrors(dctx, monoVertex)
			if e != nil {
				snap.RuntimeErrors.Error = newSnapshotDaemonError(e)
				return
			}
			remainingErrors := snapshotMaxTotalErrors
			snap.RuntimeErrors.Data = capReplicaErrors(newMonoVertexReplicaErrorDTOs(errs), &remainingErrors)
			if remainingErrors <= 0 {
				snap.Notes = append(snap.Notes, fmt.Sprintf("runtime errors truncated at %d total", snapshotMaxTotalErrors))
			}
		}()
	}

	snap.WarningEvents.ObservedAt = now()
	warnFilter := eventFilter{
		objectKind: dfv1.MonoVertexGroupVersionKind.Kind,
		objectName: monoVertex,
		eventType:  "Warning",
		limit:      snapshotMaxWarningEvents,
	}
	if events, meta, e := h.listScopedEvents(snapshotCtx, ns, warnFilter); e != nil {
		snap.WarningEvents.Error = &SnapshotSectionError{Code: SnapshotErrCodeInternal, Message: e.Error()}
	} else {
		if meta.limited {
			snap.Notes = append(snap.Notes, fmt.Sprintf("warning events truncated at %d", snapshotMaxWarningEvents))
		}
		if meta.scanCeilingReached {
			snap.Notes = append(snap.Notes, fmt.Sprintf("warning event scan reached %d event ceiling; most recent warning events may be incomplete", eventScanCeiling))
		}
		snap.WarningEvents.Data = events
	}

	c.JSON(http.StatusOK, snap)
}
