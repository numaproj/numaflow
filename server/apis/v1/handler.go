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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsversiond "k8s.io/metrics/pkg/client/clientset/versioned"
	"k8s.io/utils/pointer"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	dfv1versiond "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	"github.com/numaproj/numaflow/webhook/validator"
)

// SpecType is used to provide the type of the spec of the resource
// This is used to parse different types of specs from the request body
const (
	SpecTypePipeline = "pipeline"
	SpecTypeISB      = "isb"
	SpecTypePatch    = "patch"
)

// Constants for the validation of the pipeline
const (
	ValidTypeCreate = "valid-create"
	ValidTypeUpdate = "valid-update"
)

const DefaultNamespace = "default"

type handler struct {
	kubeClient     kubernetes.Interface
	metricsClient  *metricsversiond.Clientset
	numaflowClient dfv1clients.NumaflowV1alpha1Interface
}

// NewHandler is used to provide a new instance of the handler type
func NewHandler() (*handler, error) {
	var restConfig *rest.Config
	var err error
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, _ := os.UserHomeDir()
		kubeconfig = home + "/.kube/config"
		if _, err := os.Stat(kubeconfig); err != nil && os.IsNotExist(err) {
			kubeconfig = ""
		}
	}
	if kubeconfig != "" {
		restConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		restConfig, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to get kubeconfig, %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("Failed to get kubeclient, %w", err)
	}
	metricsClient := metricsversiond.NewForConfigOrDie(restConfig)
	numaflowClient := dfv1versiond.NewForConfigOrDie(restConfig).NumaflowV1alpha1()
	return &handler{
		kubeClient:     kubeClient,
		metricsClient:  metricsClient,
		numaflowClient: numaflowClient,
	}, nil
}

// ListNamespaces is used to provide all the namespaces that have numaflow pipelines running
func (h *handler) ListNamespaces(c *gin.Context) {
	namespaces, err := getAllNamespaces(h)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch all namespaces, %s", err.Error()))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, namespaces))
}

// GetClusterSummary summarizes information of all the namespaces in a cluster and wrapped the result in a list.
func (h *handler) GetClusterSummary(c *gin.Context) {
	namespaces, err := getAllNamespaces(h)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch cluster summary, %s", err.Error()))
		return
	}
	var clusterSummary ClusterSummaryResponse
	// Loop over the namespaces to get status
	for _, ns := range namespaces {
		// Fetch pipeline summary
		pipelines, err := getPipelines(h, ns)
		if err != nil {
			h.respondWithError(c, fmt.Sprintf("Failed to fetch cluster summary, %s", err.Error()))
			return
		}

		var pipeSummary PipelineSummary
		var pipeActiveSummary ActiveStatus
		// Loop over the pipelines and get the status
		for _, pl := range pipelines {
			if pl.Status == PipelineStatusInactive {
				pipeSummary.Inactive++
			} else {
				pipeActiveSummary.increment(pl.Status)

			}
		}
		pipeSummary.Active = pipeActiveSummary

		// Fetch ISB service summary
		isbSvcs, err := getIsbServices(h, ns)
		if err != nil {
			h.respondWithError(c, fmt.Sprintf("Failed to fetch cluster summary, %s", err.Error()))
			return
		}

		var isbSummary IsbServiceSummary
		var isbActiveSummary ActiveStatus
		// loop over the ISB services and get the status
		for _, isb := range isbSvcs {
			if isb.Status == ISBServiceStatusInactive {
				isbSummary.Inactive++
			} else {
				isbActiveSummary.increment(isb.Status)
			}
		}
		isbSummary.Active = isbActiveSummary
		clusterSummary = append(clusterSummary, NewClusterSummary(ns, pipeSummary, isbSummary))
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, clusterSummary))

}

// CreatePipeline is used to create a given pipeline
func (h *handler) CreatePipeline(c *gin.Context) {
	ns := c.Param("namespace")
	// dryRun is used to check if the operation is just a validation or an actual create
	dryRun := c.DefaultQuery("dry-run", "false")
	dryRun = strings.ToLower(dryRun)

	reqBody, err := parseSpecFromReq(c, SpecTypePipeline)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to parse request body, %s", err.Error()))
		return
	}
	pipelineSpec, ok := reqBody.(*dfv1.Pipeline)
	if !ok {
		h.respondWithError(c, "Failed to convert request body to pipeline spec")
		return
	}
	err = validateNamespace(h, pipelineSpec, ns)
	if err != nil {
		h.respondWithError(c, err.Error())
		return
	}
	pipelineSpec.Namespace = ns
	isValid := validatePipelineSpec(h, nil, pipelineSpec, ValidTypeCreate)
	if isValid != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to validate pipeline spec, %s", isValid.Error()))
		return
	}
	// if Validation flag "dryRun" is set to true, return without creating the pipeline
	if dryRun == "true" {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}

	if _, err := h.numaflowClient.Pipelines(ns).Create(context.Background(), pipelineSpec, metav1.CreateOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to create pipeline %q, %s", pipelineSpec.Name, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// ListPipelines is used to provide all the numaflow pipelines in a given namespace
func (h *handler) ListPipelines(c *gin.Context) {
	ns := c.Param("namespace")
	plList, err := getPipelines(h, ns)

	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch all pipelines for namespace %q, %s", ns, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, plList))
}

// GetPipeline is used to provide the spec of a given numaflow pipeline
func (h *handler) GetPipeline(c *gin.Context) {
	var lag int64
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	// get general pipeline info
	pl, err := h.numaflowClient.Pipelines(ns).Get(context.Background(), pipeline, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch pipeline %q namespace %q, %s", pipeline, ns, err.Error()))
		return
	}

	// get pipeline source and sink vertex
	var (
		source = make(map[string]bool)
		sink   = make(map[string]bool)
	)
	for _, vertex := range pl.Spec.Vertices {
		if vertex.IsASource() {
			source[vertex.Name] = true
		} else if vertex.IsASink() {
			sink[vertex.Name] = true
		}
	}

	// get pipeline status
	status, err := getPipelineStatus(pl)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch pipeline %q namespace %q, %s", pipeline, ns, err.Error()))
		return
	}

	// get pipeline lag
	client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch pipeline: failed to calculate lag for pipeline %q: %s", pipeline, err.Error()))
		return
	}
	defer client.Close()

	var (
		minWM int64 = math.MaxInt64
		maxWM int64 = math.MinInt64
	)
	watermarks, err := client.GetPipelineWatermarks(context.Background(), pipeline)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch pipeline: failed to calculate lag for pipeline %q: %s", pipeline, err.Error()))
		return
	}
	for _, watermark := range watermarks {
		// find the largest source vertex watermark
		if _, ok := source[*watermark.From]; ok {
			for _, wm := range watermark.Watermarks {
				if wm > maxWM {
					maxWM = wm
				}
			}
		}
		// find the smallest sink vertex watermark
		if _, ok := sink[*watermark.To]; ok {
			for _, wm := range watermark.Watermarks {
				if wm < minWM {
					minWM = wm
				}
			}
		}
	}
	// if the data hasn't arrived the sink vertex
	// use 0 instead of the initial watermark value -1
	if minWM == -1 {
		minWM = 0
	}
	lag = maxWM - minWM

	pipelineResp := NewPipelineInfo(status, &lag, pl)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, pipelineResp))
}

// UpdatePipeline is used to update a given pipeline
func (h *handler) UpdatePipeline(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")
	// dryRun is used to check if the operation is just a validation or an actual update
	dryRun := c.DefaultQuery("dry-run", "false")
	dryRun = strings.ToLower(dryRun)

	reqBody, err := parseSpecFromReq(c, SpecTypePipeline)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to parse request body, %s", err.Error()))
		return
	}

	oldSpec, err := h.numaflowClient.Pipelines(ns).Get(context.Background(), pipeline, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch pipeline %q namespace %q, %s", pipeline, ns, err.Error()))
		return
	}

	updatedSpec, ok := reqBody.(*dfv1.Pipeline)
	if !ok {
		h.respondWithError(c, "Failed to convert request body to pipeline spec")
		return
	}
	// Validate the namespace of the request
	err = validateNamespace(h, updatedSpec, ns)
	if err != nil {
		h.respondWithError(c, err.Error())
		return
	}

	// pipeline name in the URL should be same as spec name
	if pipeline != updatedSpec.Name {
		h.respondWithError(c, fmt.Sprintf("pipeline name %q is immutable", pipeline))
		return
	}

	oldSpec.Spec = updatedSpec.Spec
	updatedSpec.Namespace = ns
	isValid := validatePipelineSpec(h, oldSpec, updatedSpec, ValidTypeUpdate)
	if isValid != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update pipeline %q, %s", pipeline, isValid.Error()))
		return
	}
	// If Validation flag is set to true, return without updating the pipeline
	if dryRun == "true" {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}

	if _, err := h.numaflowClient.Pipelines(ns).Update(context.Background(), oldSpec, metav1.UpdateOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update pipeline %q, %s", pipeline, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// DeletePipeline is used to delete a given pipeline
func (h *handler) DeletePipeline(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	if err := h.numaflowClient.Pipelines(ns).Delete(context.Background(), pipeline, metav1.DeleteOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to delete pipeline %q, %s", pipeline, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// PatchPipeline is used to patch the pipeline spec to achieve operations such as "pause" and "resume"
func (h *handler) PatchPipeline(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	reqBody, err := parseSpecFromReq(c, SpecTypePatch)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to parse request body, %s", err.Error()))
		return
	}

	patchSpec, ok := reqBody.([]byte)
	if !ok {
		h.respondWithError(c, "Failed to convert request body to patch spec")
		return
	}

	if _, err := h.numaflowClient.Pipelines(ns).Patch(context.Background(), pipeline, types.MergePatchType, patchSpec, metav1.PatchOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to patch pipeline %q, %s", pipeline, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// CreateInterStepBufferService is used to create a given interstep buffer service
func (h *handler) CreateInterStepBufferService(c *gin.Context) {
	ns := c.Param("namespace")
	// dryRun is used to check if the operation is just a validation or an actual update
	dryRun := c.DefaultQuery("dry-run", "false")
	dryRun = strings.ToLower(dryRun)

	reqBody, err := parseSpecFromReq(c, SpecTypeISB)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to parse request body, %s", err.Error()))
		return
	}

	isbSpec, ok := reqBody.(*dfv1.InterStepBufferService)
	if !ok {
		h.respondWithError(c, "Failed to convert request body to interstepbuffer service spec")
		return
	}
	isValid := validateISBSVCSpec(h, nil, isbSpec, ValidTypeCreate)
	if isValid != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to create interstepbuffer service spec, %s", isValid.Error()))
		return
	}
	// If Validation flag is set to true, return without creating the ISB
	if dryRun == "true" {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}

	if _, err := h.numaflowClient.InterStepBufferServices(ns).Create(context.Background(), isbSpec, metav1.CreateOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to create interstepbuffer service %q, %s", isbSpec.Name, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// ListInterStepBufferServices is used to provide all the interstepbuffer services in a namespace
func (h *handler) ListInterStepBufferServices(c *gin.Context) {
	ns := c.Param("namespace")
	isbList, err := getIsbServices(h, ns)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch all interstepbuffer services for namespace %q, %s", ns, err.Error()))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, isbList))
}

// GetInterStepBufferService is used to provide the spec of the interstep buffer service
func (h *handler) GetInterStepBufferService(c *gin.Context) {
	ns, isbName := c.Param("namespace"), c.Param("isb-services")

	isbsvc, err := h.numaflowClient.InterStepBufferServices(ns).Get(context.Background(), isbName, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch interstepbuffer service %q namespace %q, %s", isbName, ns, err.Error()))
		return
	}

	status := ISBServiceStatusHealthy
	// TODO(API) : Get the current status of the ISB service
	// status, err := getISBServiceStatus(isb.Namespace, isb.Name)
	// if err != nil {
	//	errMsg := fmt.Sprintf("Failed to fetch interstepbuffer service %q namespace %q, %s", isb.Name, isb.Namespace, err.Error())
	//	c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
	//	return
	// }

	resp := NewISBService(status, isbsvc)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, resp))
}

// UpdateInterStepBufferService is used to update the spec of the interstep buffer service
func (h *handler) UpdateInterStepBufferService(c *gin.Context) {
	ns, isbServices := c.Param("namespace"), c.Param("isb-services")
	// dryRun is used to check if the operation is just a validation or an actual update
	dryRun := c.DefaultQuery("dry-run", "false")
	dryRun = strings.ToLower(dryRun)

	isbSVC, err := h.numaflowClient.InterStepBufferServices(ns).Get(context.Background(), isbServices, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the interstep buffer service: namespace %q isb-services %q: %s", ns, isbServices, err.Error()))
		return
	}

	requestBody, err := parseSpecFromReq(c, SpecTypeISB)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to parse request body, %s", err.Error()))
		return
	}
	var updatedSpec = requestBody.(*dfv1.InterStepBufferService)
	isValid := validateISBSVCSpec(h, isbSVC, updatedSpec, ValidTypeUpdate)
	if isValid != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to validate interstepbuffer service spec, %s", isValid.Error()))
		return
	}

	// If Validation flag is set to true, return without updating the ISB service
	if dryRun == "true" {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}

	updatedISBSvc, err := h.numaflowClient.InterStepBufferServices(ns).Update(context.Background(), isbSVC, metav1.UpdateOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update the interstep buffer service: namespace %q isb-services %q: %s", ns, isbServices, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, updatedISBSvc))
}

// DeleteInterStepBufferService is used to update the spec of the inter step buffer service
func (h *handler) DeleteInterStepBufferService(c *gin.Context) {
	ns, isbServices := c.Param("namespace"), c.Param("isb-services")

	err := h.numaflowClient.InterStepBufferServices(ns).Delete(context.Background(), isbServices, metav1.DeleteOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to delete the interstep buffer service: namespace %q isb-services %q: %s",
			ns, isbServices, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// ListPipelineBuffers is used to provide buffer information about all the pipeline vertices
func (h *handler) ListPipelineBuffers(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the Inter-Step buffers for pipeline %q: %s", pipeline, err.Error()))
		return
	}
	defer client.Close()

	buffers, err := client.ListPipelineBuffers(context.Background(), pipeline)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the Inter-Step buffers for pipeline %q: %s", pipeline, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, buffers))
}

// GetPipelineWatermarks is used to provide the head watermarks for a given pipeline
func (h *handler) GetPipelineWatermarks(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the watermarks for pipeline %q: %s", pipeline, err.Error()))
		return
	}
	defer client.Close()

	watermarks, err := client.GetPipelineWatermarks(context.Background(), pipeline)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the watermarks for pipeline %q: %s", pipeline, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, watermarks))
}

func (h *handler) respondWithError(c *gin.Context, message string) {
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(&message, nil))
}

// UpdateVertex is used to update the vertex spec
func (h *handler) UpdateVertex(c *gin.Context) {
	var (
		requestBody     dfv1.AbstractVertex
		inputVertexName = c.Param("vertex")
		pipeline        = c.Param("pipeline")
		ns              = c.Param("namespace")
	)

	pl, err := h.numaflowClient.Pipelines(ns).Get(context.Background(), pipeline, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: %s", ns,
			pipeline, inputVertexName, err.Error()))
		return
	}

	if err := c.ShouldBindJSON(&requestBody); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: %s", ns,
			pipeline, inputVertexName, err.Error()))
		return
	}

	if requestBody.Name != inputVertexName {
		h.respondWithError(c, fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: vertex name %q is immutable",
			ns, pipeline, inputVertexName, requestBody.Name))
		return
	}

	for index, vertex := range pl.Spec.Vertices {
		if vertex.Name == inputVertexName {
			if vertex.GetVertexType() != requestBody.GetVertexType() {
				h.respondWithError(c, fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: vertex type is immutable",
					ns, pipeline, inputVertexName))
				return
			}
			pl.Spec.Vertices[index] = requestBody
			break
		}
	}

	if _, err := h.numaflowClient.Pipelines(ns).Update(context.Background(), pl, metav1.UpdateOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: %s",
			ns, pipeline, inputVertexName, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, pl.Spec))
}

// GetVerticesMetrics is used to provide information about all the vertices for the given pipeline including processing rates.
func (h *handler) GetVerticesMetrics(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	pl, err := h.numaflowClient.Pipelines(ns).Get(context.Background(), pipeline, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the vertices metrics: namespace %q pipeline %q: %s", ns, pipeline, err.Error()))
		return
	}

	client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the vertices metrics: failed to get demon service client for namespace %q pipeline %q: %s", ns, pipeline, err.Error()))
		return
	}
	defer client.Close()

	var results = make(map[string][]*daemon.VertexMetrics)
	for _, vertex := range pl.Spec.Vertices {
		metrics, err := client.GetVertexMetrics(context.Background(), pipeline, vertex.Name)
		if err != nil {
			h.respondWithError(c, fmt.Sprintf("Failed to get the vertices metrics: namespace %q pipeline %q vertex %q: %s", ns, pipeline, vertex.Name, err.Error()))
			return
		}
		results[vertex.Name] = metrics
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, results))
}

// ListVertexPods is used to provide all the pods of a vertex
func (h *handler) ListVertexPods(c *gin.Context) {
	ns, pipeline, vertex := c.Param("namespace"), c.Param("pipeline"), c.Param("vertex")

	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	pods, err := h.kubeClient.CoreV1().Pods(ns).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipeline, dfv1.KeyVertexName, vertex),
		Limit:         limit,
		Continue:      c.Query("continue"),
	})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get a list of pods: namespace %q pipeline %q vertex %q: %s",
			ns, pipeline, vertex, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, pods.Items))
}

// ListPodsMetrics is used to provide a list of all metrics in all the pods
func (h *handler) ListPodsMetrics(c *gin.Context) {
	ns := c.Param("namespace")

	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	metrics, err := h.metricsClient.MetricsV1beta1().PodMetricses(ns).List(context.Background(), metav1.ListOptions{
		Limit:    limit,
		Continue: c.Query("continue"),
	})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get a list of pod metrics in namespace %q: %s", ns, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, metrics.Items))
}

// PodLogs is used to provide the logs of a given container in pod
func (h *handler) PodLogs(c *gin.Context) {
	ns, pod := c.Param("namespace"), c.Param("pod")

	// parse the query parameters
	tailLines := h.parseTailLines(c.Query("tailLines"))
	logOptions := &corev1.PodLogOptions{
		Container: c.Query("container"),
		Follow:    c.Query("follow") == "true",
		TailLines: tailLines,
	}

	stream, err := h.kubeClient.CoreV1().Pods(ns).GetLogs(pod, logOptions).Stream(c)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get pod logs: %s", err.Error()))
		return
	}
	defer stream.Close()

	// Stream the logs back to the client
	h.streamLogs(c, stream)
}

func (h *handler) parseTailLines(query string) *int64 {
	if query == "" {
		return nil
	}

	x, _ := strconv.ParseInt(query, 10, 64)
	return pointer.Int64(x)
}

func (h *handler) streamLogs(c *gin.Context, stream io.ReadCloser) {
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		_, _ = c.Writer.Write(scanner.Bytes())
		_, _ = c.Writer.WriteString("\n")
		c.Writer.Flush()
	}
}

// GetNamespaceEvents gets a list of events for the given namespace.
func (h *handler) GetNamespaceEvents(c *gin.Context) {
	ns := c.Param("namespace")

	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	events, err := h.kubeClient.CoreV1().Events(ns).List(context.Background(), metav1.ListOptions{
		Limit:    limit,
		Continue: c.Query("continue"),
	})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get a list of events: namespace %q: %s", ns, err.Error()))
		return
	}

	var (
		response          []K8sEventsResponse
		defaultTimeObject time.Time
	)

	for _, event := range events.Items {
		if event.LastTimestamp.Time == defaultTimeObject {
			continue
		}
		var newEvent = NewK8sEventsResponse(event.LastTimestamp.UnixMilli(), event.Type, event.InvolvedObject.Kind, event.InvolvedObject.Name, event.Reason, event.Message)
		response = append(response, newEvent)
	}

	// sort the events by timestamp
	// from most recent events to older events
	sort.Slice(response, func(i int, j int) bool {
		return response[i].TimeStamp >= response[j].TimeStamp
	})

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, response))
}

// GetPipelineStatus returns the pipeline status. It is based on Health and Criticality.
// Health can be "healthy (0) | unhealthy (1) | paused (3) | unknown (4)".
// Health here indicates pipeline's ability to process messages.
// A backlogged pipeline can be healthy even though it has an increasing back-pressure. Health purely means it is up and running.
// Pipelines health will be the max(health) based of each vertex's health
// Criticality on the other end shows whether the pipeline is working as expected.
// It represents the pending messages, lags, etc.
// Criticality can be "ok (0) | warning (1) | critical (2)".
// Health and Criticality are different because ...?
func (h *handler) GetPipelineStatus(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, "working on it")
}

// getAllNamespaces is a utility used to fetch all the namespaces in the cluster
func getAllNamespaces(h *handler) ([]string, error) {
	l, err := h.numaflowClient.Pipelines("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	m := make(map[string]bool)
	for _, pl := range l.Items {
		m[pl.Namespace] = true
	}

	isbsvc, err := h.numaflowClient.InterStepBufferServices("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, isb := range isbsvc.Items {
		m[isb.Namespace] = true
	}
	var namespaces []string
	for k := range m {
		namespaces = append(namespaces, k)
	}
	return namespaces, nil
}

// getPipelines is a utility used to fetch all the pipelines in a given namespace
func getPipelines(h *handler, namespace string) (Pipelines, error) {
	plList, err := h.numaflowClient.Pipelines(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var pipelineList Pipelines
	for _, pl := range plList.Items {
		status, err := getPipelineStatus(&pl)
		if err != nil {
			return nil, err
		}
		// NOTE: we only calculate pipeline lag for get single pipeline API
		// to avoid massive gRPC calls
		resp := NewPipelineInfo(status, nil, &pl)
		pipelineList = append(pipelineList, resp)
	}
	return pipelineList, nil
}

// getIsbServices is used to fetch all the interstepbuffer services in a given namespace
func getIsbServices(h *handler, namespace string) (ISBServices, error) {
	isbSvcs, err := h.numaflowClient.InterStepBufferServices(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var isbList ISBServices
	for _, isb := range isbSvcs.Items {
		status := ISBServiceStatusHealthy
		// TODO(API) : Get the current status of the ISB service
		// status, err := getISBServiceStatus(isb.Namespace, isb.Name)
		// if err != nil {
		//	errMsg := fmt.Sprintf("Failed to fetch interstepbuffer service %q namespace %q, %s", isb.Name, isb.Namespace, err.Error())
		//	c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		//	return
		// }
		resp := NewISBService(status, &isb)
		isbList = append(isbList, resp)
	}
	return isbList, nil
}

// parseSpecFromReq is used to parse the request body and return the spec
// based on the type of request
func parseSpecFromReq(c *gin.Context, specType string) (interface{}, error) {
	var reqBody interface{}
	jsonData, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, err
	}
	if specType == SpecTypePipeline {
		reqBody = &dfv1.Pipeline{}

	} else if specType == SpecTypeISB {
		reqBody = &dfv1.InterStepBufferService{}
	} else if specType == SpecTypePatch {
		return jsonData, nil
	}
	err = json.Unmarshal(jsonData, &reqBody)
	if err != nil {
		return nil, err
	}
	return reqBody, nil
}

// GetPipelineStatus is used to provide the status of a given pipeline
// TODO(API): Change the Daemon service to return the consolidated status of the pipeline
// to save on multiple calls to the daemon service
func getPipelineStatus(pipeline *dfv1.Pipeline) (string, error) {
	retStatus := PipelineStatusHealthy
	// Check if the pipeline is paused, if so, return inactive status
	if pipeline.Spec.Lifecycle.GetDesiredPhase() == dfv1.PipelinePhasePaused {
		retStatus = PipelineStatusInactive
	} else if pipeline.Spec.Lifecycle.GetDesiredPhase() == dfv1.PipelinePhaseRunning {
		retStatus = PipelineStatusHealthy
	} else if pipeline.Spec.Lifecycle.GetDesiredPhase() == dfv1.PipelinePhaseFailed {
		retStatus = PipelineStatusCritical
	}
	// ns := pipeline.Namespace
	// pipeName := pipeline.Name
	// client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeName))
	// if err != nil {
	//	return "", err
	// }
	// defer func() {
	//	_ = client.Close()
	// }()
	// l, err := client.GetPipelineStatus(context.Background(), pipeName)
	// if err != nil {
	//	return "", err
	// }
	// retStatus := PipelineStatusHealthy
	// // TODO(API) : Check for warning status?
	// if *l.Status != "OK" {
	//	retStatus = PipelineStatusCritical
	// }
	// // Check if the pipeline is paused, if so, return inactive status
	// if pipeline.Spec.Lifecycle.GetDesiredPhase() == dfv1.PipelinePhasePaused {
	//	retStatus = PipelineStatusInactive
	// }
	return retStatus, nil
}

// validatePipelineSpec is used to validate the pipeline spec during create and update
func validatePipelineSpec(h *handler, oldPipeline *dfv1.Pipeline, newPipeline *dfv1.Pipeline, validType string) error {
	ns := newPipeline.Namespace
	pipeClient := h.numaflowClient.Pipelines(ns)
	isbClient := h.numaflowClient.InterStepBufferServices(ns)
	valid := validator.NewPipelineValidator(h.kubeClient, pipeClient, isbClient, oldPipeline, newPipeline)
	var resp *admissionv1.AdmissionResponse
	switch validType {
	case ValidTypeCreate:
		resp = valid.ValidateCreate(context.Background())
	case ValidTypeUpdate:
		resp = valid.ValidateUpdate(context.Background())
	}
	if !resp.Allowed {
		errMsg := fmt.Errorf("%s", resp.Result.Message)
		return errMsg
	}
	return nil
}

// validateISBSVCSpec is used to validate the ISB service spec
func validateISBSVCSpec(h *handler, prevSpec *dfv1.InterStepBufferService,
	newSpec *dfv1.InterStepBufferService, validType string) error {
	ns := newSpec.Namespace
	isbClient := h.numaflowClient.InterStepBufferServices(ns)
	valid := validator.NewISBServiceValidator(h.kubeClient, isbClient, prevSpec, newSpec)
	var resp *admissionv1.AdmissionResponse
	switch validType {
	case ValidTypeCreate:
		resp = valid.ValidateCreate(context.Background())
	case ValidTypeUpdate:
		resp = valid.ValidateUpdate(context.Background())
	}
	if !resp.Allowed {
		errMsg := fmt.Errorf("%s", resp.Result.Message)
		return errMsg
	}
	return nil
}

// validateNamespace is used to validate the namespace for a pipeline spec
// For a request, the namespace provided as parameter should be same as the namespace in the pipeline spec
func validateNamespace(h *handler, pipeline *dfv1.Pipeline, ns string) error {
	if pipeline.Namespace != "" && pipeline.Namespace != ns {
		errMsg := fmt.Errorf("namespace mismatch, expected %s", ns)
		return errMsg
	}
	return nil
}

func daemonSvcAddress(ns, pipeline string) string {
	return fmt.Sprintf("%s.%s.svc:%d", fmt.Sprintf("%s-daemon-svc", pipeline), ns, dfv1.DaemonServicePort)
}
