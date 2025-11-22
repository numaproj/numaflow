package v1

import (
	"context"
	"fmt"
	"time"

	evictCache "github.com/hashicorp/golang-lru/v2/expirable"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

const (
	// resourceCacheRefreshDuration is the duration after which the resource status cache is refreshed
	resourceCacheRefreshDuration = 30 * time.Second
)

// resourceHealthResponse is the response returned by the health check API
type resourceHealthResponse struct {
	// Status is the overall resource status of the corresponding resource
	Status string `json:"status"`
	// Message is the error message if any
	Message string `json:"message"`
	// Code is the status code for the resource health
	Code string `json:"code"`
}

// HealthChecker is the struct to hold the resource status cache for the
// pipeline and Mono Vertex
type HealthChecker struct {
	// pipelineResourceStatusCache is a cache to store the pipeline resource status
	pipelineResourceStatusCache *evictCache.LRU[string, *resourceHealthResponse]
	// monoVtxResourceStatusCache is a cache to store the Mono Vertex resource status
	monoVtxResourceStatusCache *evictCache.LRU[string, *resourceHealthResponse]
	log                        *zap.SugaredLogger
}

// NewHealthChecker is used to create a new health checker
func NewHealthChecker(ctx context.Context) *HealthChecker {
	c := evictCache.NewLRU[string, *resourceHealthResponse](500, nil, resourceCacheRefreshDuration)
	mvCache := evictCache.NewLRU[string, *resourceHealthResponse](500, nil, resourceCacheRefreshDuration)
	return &HealthChecker{
		pipelineResourceStatusCache: c,
		monoVtxResourceStatusCache:  mvCache,
		log:                         logging.FromContext(ctx),
	}
}

// getPipelineResourceHealth is used to provide the overall vertex health and status of the pipeline
// This first check if the pipeline status is cached, if not, it checks for the current pipeline status
// and caches it.
func (hc *HealthChecker) getPipelineResourceHealth(h *handler, ns string,
	pipeline string) (*resourceHealthResponse, error) {

	// create a cache key for the pipeline
	// It is a combination of namespace and pipeline name
	// In the form of <namespace>-<pipeline>
	cacheKey := fmt.Sprintf("%s-%s", ns, pipeline)

	// check if the pipeline status is cached
	if status, ok := hc.pipelineResourceStatusCache.Get(cacheKey); ok {
		hc.log.Info("Pipeline status from cache: ", status)
		return status, nil
	}
	// if not present in cache, check for the current pipeline status
	status, err := checkVertexLevelHealth(h, ns, pipeline, hc.log)
	if err != nil {
		return status, err
	}
	// update cache with the new pipeline status
	hc.pipelineResourceStatusCache.Add(cacheKey, status)

	return status, nil
}

// checkVertexLevelHealth is used to provide the overall vertex health and status of the pipeline
// They can be of the following types:
// 1. Healthy: All the vertices are healthy
// 2. Unhealthy: One or more vertices are unhealthy
// 3. Paused: The pipeline is paused
// 4. Unknown: The pipeline is in an unknown state
// 5. Killed: The pipeline is killed
// To check for vertex level status we need to check for two things,
// 1) the number of replicas running in the vertex
// are equal to the number of desired replicas and the pods are in running state
// 2) If all the containers in the pod are in running state
// if any of the above conditions are not met, the vertex is unhealthy
func checkVertexLevelHealth(h *handler, ns string,
	pipeline string, log *zap.SugaredLogger) (*resourceHealthResponse, error) {
	// get the pipeline object
	pl, err := h.numaflowClient.Pipelines(ns).Get(context.Background(), pipeline, metav1.GetOptions{})
	// if error return unknown status
	if err != nil {
		return &resourceHealthResponse{
			Status:  dfv1.PipelineStatusUnknown,
			Message: fmt.Sprintf("Error in getting pipeline %q status: %v", pipeline, err),
			Code:    "V6",
		}, err
	}

	// if the pipeline is paused, return inactive status
	// this cannot be checked at individual vertex level, hence needs to be checked here
	if pl.GetDesiredPhase() == dfv1.PipelinePhasePaused {
		return &resourceHealthResponse{
			Status:  dfv1.PipelineStatusInactive,
			Message: fmt.Sprintf("Pipeline %q is paused", pipeline),
			Code:    "V7",
		}, nil
	}

	// if the pipeline is killed, return killed status
	// this cannot be checked at individual vertex level, hence needs to be checked here
	if pl.GetDesiredPhase() == dfv1.PipelinePhaseDeleting {
		return &resourceHealthResponse{
			Status:  dfv1.PipelineStatusDeleting,
			Message: fmt.Sprintf("Pipeline %q is killed", pipeline),
			Code:    "V8",
		}, nil
	}

	// get the list of vertices in the pipeline
	vertices := pl.Spec.Vertices

	// Iterate over all the vertices
	for _, vertex := range vertices {
		vertexName := fmt.Sprintf("%s-%s", pipeline, vertex.Name)
		// fetch the current spec of the vertex
		v, err := h.numaflowClient.Vertices(ns).Get(context.Background(), vertexName, metav1.GetOptions{})
		if err != nil {
			return &resourceHealthResponse{
				Status:  dfv1.PipelineStatusUnknown,
				Message: fmt.Sprintf("Error in getting vertex %q status: %v", vertexName, err),
				Code:    "V6",
			}, err
		}
		ok, resp, err := isVertexHealthy(h, ns, pipeline, v, vertex.Name)
		if err != nil {
			resp.Status = dfv1.PipelineStatusUnknown
			return resp, err
		}
		if !ok {
			log.Infof("vertex %q is unhealthy: %s", vertex.Name, resp)
			resp.Status = dfv1.PipelineStatusUnhealthy
			return resp, nil
		}
	}
	// if all the vertices are healthy, return healthy status
	return &resourceHealthResponse{
		Status:  dfv1.PipelineStatusHealthy,
		Message: fmt.Sprintf("Pipeline %q is healthy", pipeline),
		Code:    "V4",
	}, nil
}

// isVertexHealthy is used to check if the vertex is healthy or not
// It checks for the following:
// 1) If the vertex is in running state
// 2) the number of replicas running in the vertex
// are equal to the number of desired replicas and the pods are in running state
// 3) If all the containers in the pod are in running state
// if any of the above conditions are not met, the vertex is unhealthy
// Based on the above conditions, it returns the status code and message
func isVertexHealthy(h *handler, ns string, pipeline string, vertex *dfv1.Vertex,
	vertexName string) (bool, *resourceHealthResponse, error) {
	// check if the vertex is in running state
	if vertex.Status.Phase != dfv1.VertexPhaseRunning {
		// check if the number of replicas running in the vertex
		// are equal to the number of desired replicas
		if int(vertex.Status.Replicas) != int(vertex.Status.DesiredReplicas) {
			return false, &resourceHealthResponse{
				Message: fmt.Sprintf("Vertex %q has %d replicas running, "+
					"expected %d", vertex.Name, vertex.Status.Replicas, vertex.Status.DesiredReplicas),
				Code: "V9",
			}, nil
		}
		// Else return the error message from the status
		return false, &resourceHealthResponse{
			Message: fmt.Sprintf("Error in vertex %s", vertex.Status.Message),
			Code:    "V2",
		}, nil
	}

	// Get all the pods for the given vertex
	pods, err := h.kubeClient.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName,
			pipeline, dfv1.KeyVertexName, vertexName),
	})
	if err != nil {
		return false, &resourceHealthResponse{
			Message: fmt.Sprintf("Error in getting pods for vertex %q: %v", vertexName, err),
			Code:    "V6",
		}, err
	}
	// Iterate over all the pods, and verify if all the containers in the pod are in running state
	for _, pod := range pods.Items {
		// Iterate over all the containers in the pod
		for _, containerStatus := range pod.Status.ContainerStatuses {
			// if the container is not in running state, return false
			if containerStatus.State.Running == nil {
				return false, &resourceHealthResponse{
					Message: fmt.Sprintf("Container %q in pod %q is not running",
						containerStatus.Name, pod.Name),
					Code: "V3",
				}, nil
			}
		}
	}
	return true, &resourceHealthResponse{
		Message: fmt.Sprintf("Vertex %q is healthy", vertexName),
		Code:    "V1",
	}, nil
}

// getMonoVtxResourceHealth is used to provide the overall resouce health and status of the Mono Vertex
// This first check if the Mono Vertex status is cached, if not,
// it checks for the current Mono Vertex status
func (hc *HealthChecker) getMonoVtxResourceHealth(h *handler, ns string,
	monoVtx string) (*resourceHealthResponse, error) {

	// create a cache key for the Mono Vertex
	// It is a combination of namespace and Mono Vertex name
	// In the form of <namespace>-<monoVtx>
	cacheKey := fmt.Sprintf("%s-%s", ns, monoVtx)

	// check if the Mono Vertex status is cached
	if status, ok := hc.monoVtxResourceStatusCache.Get(cacheKey); ok {
		hc.log.Info("Mono Vertex status from cache: ", status)
		return status, nil
	}
	// if not present in cache, check for the current Mono Vertex status
	status, err := checkMonoVtxHealth(h, ns, monoVtx)
	if err != nil {
		return nil, err
	}
	// update cache with the new Mono Vertex status
	hc.monoVtxResourceStatusCache.Add(cacheKey, status)

	return status, nil
}

// checkMonoVtxHealth is used to provide the overall Mono Vertex health and status of the Mono Vertex
// They can be of the following types:
// 1. Healthy: The Mono Vertex is healthy
// 2. Unhealthy: The Mono Vertex is unhealthy
// 3. Paused: The Mono Vertex is paused (Not supported right now)
// 4. Unknown: The Mono Vertex is in an unknown state
// We use the kubernetes client to get the spec of the MonoVertex and
// then check its status to derive the resource health status

// We perform the following checks:
// 1) Check the `phase“ in the Status field of the spec, it should be “Running”
// 2) Check if the `conditions` field of the spec, all of them shoudl be true
func checkMonoVtxHealth(h *handler, ns string, monoVtx string) (*resourceHealthResponse, error) {
	// fetch the current spec of the Mono Vertex
	v, err := h.numaflowClient.MonoVertices(ns).Get(context.Background(), monoVtx, metav1.GetOptions{})
	// if there is an error fetching the spec, return an error
	// with status unknown
	if err != nil {
		return &resourceHealthResponse{
			Status:  dfv1.MonoVertexStatusUnknown,
			Message: fmt.Sprintf("error in getting Mono Vertex %q status: %v", monoVtx, err),
			Code:    "M4",
		}, err
	}
	// check if the Mono vertex is healthy or not through the status field of the spec
	isMvtxHealthy := v.Status.IsHealthy()
	if !isMvtxHealthy {
		return &resourceHealthResponse{
			Status:  dfv1.MonoVertexStatusUnhealthy,
			Message: fmt.Sprintf("mono vertex %q is unhealthy: %s:%s", monoVtx, v.Status.Message, v.Status.Reason),
			Code:    "M2",
		}, nil
	}
	return &resourceHealthResponse{
		Status:  dfv1.MonoVertexStatusHealthy,
		Message: fmt.Sprintf("mono vertex %q is healthy", monoVtx),
		Code:    "M1",
	}, nil
}
