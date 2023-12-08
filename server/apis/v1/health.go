package v1

import (
	"fmt"
	"time"

	evictCache "github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/net/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/logging"
)

const (
	// vertexCacheRefreshDuration is the duration after which the vertex status cache is refreshed
	vertexCacheRefreshDuration = 30 * time.Second
)

// vertexHealthResponse is the response returned by the vertex health check API
type vertexHealthResponse struct {
	// Status is the overall vertex status of the pipeline
	Status string `json:"status"`
	// Message is the error message if any
	Message string `json:"message"`
}

type HealthChecker struct {
	vertexStatusCache *evictCache.LRU[string, *vertexHealthResponse]
}

func NewHealthChecker() *HealthChecker {
	c := evictCache.NewLRU[string, *vertexHealthResponse](500, nil, vertexCacheRefreshDuration)
	return &HealthChecker{
		vertexStatusCache: c,
	}
}

// getPipelineVertexHealth is used to provide the overall vertex health and status of the pipeline
// This first check if the pipeline status is cached, if not, it checks for the current pipeline status
// and caches it.
func (hc *HealthChecker) getPipelineVertexHealth(h *handler, ns string, pipeline string) (*vertexHealthResponse, error) {
	ctx := context.Background()
	log := logging.FromContext(ctx)

	// create a cache key for the pipeline
	// It is a combination of namespace and pipeline name
	// In the form of <namespace>-<pipeline>
	cacheKey := fmt.Sprintf("%s-%s", ns, pipeline)

	// check if the pipeline status is cached
	if status, ok := hc.vertexStatusCache.Get(cacheKey); ok {
		log.Info("pipeline status from cache: ", status)
		return status, nil
	}
	// if not, get the pipeline status
	status, err := checkVertexLevelHealth(ctx, h, ns, pipeline)
	if err != nil {
		return &vertexHealthResponse{
			Status:  PipelineStatusUnknown,
			Message: fmt.Sprintf("error in getting pipeline %q status: %v", pipeline, err),
		}, err
	}
	// cache the pipeline status
	hc.vertexStatusCache.Add(cacheKey, status)

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
func checkVertexLevelHealth(ctx context.Context, h *handler, ns string, pipeline string) (*vertexHealthResponse, error) {
	log := logging.FromContext(ctx)
	// check if the pipeline is paused, if so, return paused status
	pl, err := h.numaflowClient.Pipelines(ns).Get(context.Background(), pipeline, metav1.GetOptions{})
	// if error return unknown status
	if err != nil {
		return &vertexHealthResponse{
			Status:  PipelineStatusUnknown,
			Message: fmt.Sprintf("error in getting pipeline %q status: %v", pipeline, err),
		}, err
	}

	// if the pipeline is paused, return inactive status
	// this cannot be checked at individual vertex level, hence needs to be checked here
	if pl.Spec.Lifecycle.GetDesiredPhase() == dfv1.PipelinePhasePaused {
		return &vertexHealthResponse{
			Status:  PipelineStatusInactive,
			Message: fmt.Sprintf("pipeline %q is paused", pipeline),
		}, nil
	}

	// if the pipeline is killed, return killed status
	// this cannot be checked at individual vertex level, hence needs to be checked here
	// TODO(Health): Check if this is correct?
	if pl.Spec.Lifecycle.GetDesiredPhase() == dfv1.PipelinePhaseDeleting {
		return &vertexHealthResponse{
			Status:  PipelineStatusDeleting,
			Message: fmt.Sprintf("pipeline %q is killed", pipeline),
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
			return &vertexHealthResponse{
				Status:  PipelineStatusUnknown,
				Message: fmt.Sprintf("error in getting vertex %q status: %v", vertexName, err),
			}, err
		}
		ok, issue, err := isVertexHealthy(h, ns, pipeline, v, vertex.Name)
		if err != nil {
			return &vertexHealthResponse{
				Status:  PipelineStatusUnknown,
				Message: fmt.Sprintf("error in getting vertex %q status: %v", vertexName, err),
			}, err
		}
		if !ok {
			log.Infof("vertex %q is unhealthy: %s", vertex.Name, issue)
			return &vertexHealthResponse{
				Status:  PipelineStatusUnhealthy,
				Message: issue,
			}, nil
		}
	}
	// if all the vertices are healthy, return healthy status
	return &vertexHealthResponse{
		Status:  PipelineStatusHealthy,
		Message: fmt.Sprintf("pipeline %q is healthy", pipeline),
	}, nil
}

// isVertexHealthy is used to check if the number of replicas running in the vertex
// are equal to the number of desired replicas and the pods are in running state.
// We first check if the vertex is in running state, if not, return the error message from the status
func isVertexHealthy(h *handler, ns string, pipeline string, vertex *dfv1.Vertex, vertexName string) (bool, string, error) {
	log := logging.FromContext(context.Background())
	// check if the vertex is in running state
	log.Info("vertex status: ", vertex.Name, vertex.Status.Phase)
	if vertex.Status.Phase != dfv1.VertexPhaseRunning {
		//// check if the number of replicas running in the vertex
		//// are equal to the number of desired replicas
		//if int(vertex.Status.Replicas) != vertex.GetReplicas() {
		//	return false, fmt.Sprintf("vertex %q has %d replicas running, "+
		//		"expected %d", vertex.Name, vertex.Status.Replicas, vertex.GetReplicas()), nil
		//}
		// Else return the error message from the status
		return false, fmt.Sprintf("error in vertex %s", vertex.Status.Message), nil
		//setState(D1) -> D1 vertex is failing
	}

	// Get all the pods for the given vertex
	pods, err := h.kubeClient.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipeline, dfv1.KeyVertexName, vertexName),
	})
	if err != nil {
		return false, "", err
	}
	log.Info("number of pods: ", len(pods.Items))
	// Iterate over all the pods, and verify if all the containers in the pod are in running state
	for _, pod := range pods.Items {
		// Iterate over all the containers in the pod
		for _, containerStatus := range pod.Status.ContainerStatuses {
			// if the container is not in running state, return false
			if containerStatus.State.Running == nil {
				return false, fmt.Sprintf("container %q in pod %q is not ready",
					containerStatus.Name, pod.Name), nil
				// Set(D2) D2->  container is failing
			}
		}
	}
	return true, "", nil
}

// getCombinedHealthStatus is used to provide the overall health of the pipeline
// It combines the health status of all the vertices in the pipeline along with the data criticality status
// to provide the overall health of the pipeline
// It takes vertex health status and data criticality status as input along with any error message string to be returned
// The final state is returned as a string
func (hc *HealthChecker) getCombinedHealthStatus(vertexHealthStatus string, dataCriticalityStatus string,
	vertexMessage string, dataMessage string) (string, string) {
	// Join the vertex health status and data criticality status
	resp := fmt.Sprintf("%s-%s", vertexHealthStatus, dataCriticalityStatus)
	// Join the vertex message and data message
	message := fmt.Sprintf("%s:%s", vertexMessage, dataMessage)
	return resp, message

}
