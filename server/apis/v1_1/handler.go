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

package v1_1

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
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
		errMsg := fmt.Sprintf("Failed to fetch all namespaces, %v", err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, namespaces))
}

// GetClusterSummary summarizes information of all the namespaces in a cluster and wrapped the result in a list.
func (h *handler) GetClusterSummary(c *gin.Context) {
	namespaces, err := getAllNamespaces(h)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to fetch cluster summary, %v", err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	var clusterSummary ClusterSummaryResponse
	// Loop over the namespaces to get status
	for _, ns := range namespaces {
		// Fetch pipeline summary
		pipelines, err := getPipelines(h, ns)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to fetch cluster summary, %v", err.Error())
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
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
			errMsg := fmt.Sprintf("Failed to fetch cluster summary, %v", err.Error())
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
			return
		}
		var isbSummary IsbServiceSummary
		var isbActiveSummary ActiveStatus
		// Loop over the ISB services and get the status
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
	reqBody, err := parseSpecFromReq(c, SpecTypePipeline)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body, %v", err.Error())
		c.JSON(http.StatusOK, errMsg)
		return
	}
	// Convert reqBody to pipeline spec
	var pipelineSpec = reqBody.(*dfv1.Pipeline)

	_, err = h.numaflowClient.Pipelines(ns).Create(context.Background(), pipelineSpec, metav1.CreateOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to create pipeline %q, %v", pipelineSpec.Name, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// ListPipelines is used to provide all the numaflow pipelines in a given namespace
func (h *handler) ListPipelines(c *gin.Context) {
	ns := c.Param("namespace")
	plList, err := getPipelines(h, ns)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to fetch all pipelines for namespace %q, %v",
			c.Param("namespace"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, plList))
}

// GetPipeline is used to provide the spec of a given numaflow pipeline
func (h *handler) GetPipeline(c *gin.Context) {
	ns := c.Param("namespace")
	pipeline := c.Param("pipeline")
	pl, err := h.numaflowClient.Pipelines(ns).Get(context.Background(),
		pipeline, metav1.GetOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to fetch pipeline %q namespace %q, %v",
			pipeline,
			ns,
			err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	status, err := getPipelineStatus(pl)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to fetch pipeline %q namespace %q, %v",
			pipeline,
			ns,
			err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	pipelineResp := NewPipelineInfo(status, pl)
	c.JSON(http.StatusOK, pipelineResp)
}

// UpdatePipeline is used to update a given pipeline
func (h *handler) UpdatePipeline(c *gin.Context) {
	ns := c.Param("namespace")
	pipeline := c.Param("pipeline")
	reqBody, err := parseSpecFromReq(c, SpecTypePipeline)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body, %v", err.Error())
		c.JSON(http.StatusOK, errMsg)
		return
	}
	pl, err := h.numaflowClient.Pipelines(ns).Get(context.Background(),
		pipeline, metav1.GetOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to patch pipeline %q namespace %q, %v",
			pipeline,
			ns,
			err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	var pipelineSpec = reqBody.(*dfv1.Pipeline)
	pl.Spec = pipelineSpec.Spec
	fmt.Println("DEBUG", pl.ResourceVersion)
	_, err = h.numaflowClient.Pipelines(ns).Update(context.Background(), pl, metav1.UpdateOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to update pipeline %q, %v", pipeline, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// DeletePipeline is used to delete a given pipeline
func (h *handler) DeletePipeline(c *gin.Context) {
	ns := c.Param("namespace")
	pipeline := c.Param("pipeline")
	err := h.numaflowClient.Pipelines(ns).Delete(context.Background(), pipeline, metav1.DeleteOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to delete pipeline %q, %v", pipeline, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// PatchPipeline is used to patch the pipeline spec to achieve operations such as "pause" and "resume"
func (h *handler) PatchPipeline(c *gin.Context) {
	ns := c.Param("namespace")
	pipeline := c.Param("pipeline")
	reqBody, err := parseSpecFromReq(c, SpecTypePatch)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body, %v", err.Error())
		c.JSON(http.StatusOK, errMsg)
		return
	}
	patchSpec := reqBody.([]byte)
	_, err = h.numaflowClient.Pipelines(ns).Patch(context.Background(), pipeline, types.MergePatchType,
		patchSpec, metav1.PatchOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to patch pipeline %q, %v", pipeline, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// CreateInterStepBufferService is used to create a given interstep buffer service
func (h *handler) CreateInterStepBufferService(c *gin.Context) {
	ns := c.Param("namespace")
	reqBody, err := parseSpecFromReq(c, SpecTypeISB)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body, %v", err.Error())
		c.JSON(http.StatusOK, errMsg)
		return
	}
	var isbSpec = reqBody.(*dfv1.InterStepBufferService)
	_, err = h.numaflowClient.InterStepBufferServices(ns).Create(context.Background(), isbSpec, metav1.CreateOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to create interstepbuffer service %q, %v", isbSpec.Name, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// ListInterStepBufferServices is used to provide all the interstepbuffer services in a namespace
func (h *handler) ListInterStepBufferServices(c *gin.Context) {
	ns := c.Param("namespace")
	isbList, err := getIsbServices(h, ns)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to fetch all interstepbuffer services for namespace %q, %v", ns, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, isbList))
}

// GetInterStepBufferService is used to provide the spec of the interstep buffer service
func (h *handler) GetInterStepBufferService(c *gin.Context) {
	isbName := c.Param("isb-services")
	ns := c.Param("namespace")
	isbsvc, err := h.numaflowClient.InterStepBufferServices(ns).Get(context.Background(), isbName, metav1.GetOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to fetch interstepbuffer service %q namespace %q, %v",
			isbName,
			ns, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	status := ISBServiceStatusHealthy
	// TODO(API) : Get the current status of the ISB service
	// status, err := getISBServiceStatus(isb.Namespace, isb.Name)
	// if err != nil {
	//	errMsg := fmt.Sprintf("Failed to fetch interstepbuffer service %q namespace %q, %v", isb.Name, isb.Namespace, err.Error())
	//	c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
	//	return
	// }
	resp := NewISBService(status, isbsvc)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, resp))
}

// UpdateInterStepBufferService is used to update the spec of the interstep buffer service
func (h *handler) UpdateInterStepBufferService(c *gin.Context) {
	isbSVC, err := h.numaflowClient.InterStepBufferServices(c.Param("namespace")).Get(context.Background(), c.Param("isb-services"), metav1.GetOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get the interstep buffer service: namespace %q isb-services %q: %v", c.Param("namespace"), c.Param("isb-services"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	var requestBody dfv1.InterStepBufferServiceSpec
	err = json.NewDecoder(c.Request.Body).Decode(&requestBody)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to update the interstep buffer service: namespace %q isb-services %q: %v", c.Param("namespace"), c.Param("isb-services"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	if requestBody.Redis != nil {
		errMsg := fmt.Sprintf("Failed to update the interstep buffer service: namespace %q isb-services %q: updating redis isbSVC is not supported.", c.Param("namespace"), c.Param("isb-services"))
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	} else if requestBody.JetStream != nil {
		if *(requestBody.JetStream.Replicas) < 3 {
			errMsg := fmt.Sprintf("Failed to update the interstep buffer service: namespace %q isb-services %q: minimum number of replicas is 3.", c.Param("namespace"), c.Param("isb-services"))
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
			return
		}
		if *(requestBody.JetStream.Replicas) > 5 {
			errMsg := fmt.Sprintf("Failed to update the interstep buffer service: namespace %q isb-services %q: maximum number of replicas is 5.", c.Param("namespace"), c.Param("isb-services"))
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
			return
		}
		// TODO: currently we can only update the replica
		isbSVC.Spec.JetStream.Replicas = requestBody.JetStream.Replicas
	}
	updatedISBSvc, err := h.numaflowClient.InterStepBufferServices(c.Param("namespace")).Update(context.Background(), isbSVC, metav1.UpdateOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to update the interstep buffer service: namespace %q isb-services %q: %v", c.Param("namespace"), c.Param("isb-services"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, updatedISBSvc))
}

// DeleteInterStepBufferService is used to update the spec of the interstep buffer service
func (h *handler) DeleteInterStepBufferService(c *gin.Context) {
	err := h.numaflowClient.InterStepBufferServices(c.Param("namespace")).Delete(context.Background(), c.Param("isb-services"), metav1.DeleteOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to delete the interstep buffer service: namespace %q isb-services %q: %v", c.Param("namespace"), c.Param("isb-services"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// ListPipelineBuffers is used to provide buffer information about all the pipeline vertices
func (h *handler) ListPipelineBuffers(c *gin.Context) {
	ns := c.Param("namespace")
	pipeline := c.Param("pipeline")
	client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get the Inter-Step buffers for pipeline %q: %v", c.Param("pipeline"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	defer func() {
		_ = client.Close()
	}()
	l, err := client.ListPipelineBuffers(context.Background(), pipeline)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get the Inter-Step buffers for pipeline %q: %v", c.Param("pipeline"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, l))
}

// GetPipelineWatermarks is used to provide the head watermarks for a given pipeline
func (h *handler) GetPipelineWatermarks(c *gin.Context) {
	ns := c.Param("namespace")
	pipeline := c.Param("pipeline")
	client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get the watermarks for pipeline %q: %v", c.Param("pipeline"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	defer func() {
		_ = client.Close()
	}()
	l, err := client.GetPipelineWatermarks(context.Background(), pipeline)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get the watermarks for pipeline %q: %v", c.Param("pipeline"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, l))
}

// UpdateVertex is used to provide the vertex spec
func (h *handler) UpdateVertex(c *gin.Context) {
	var (
		requestBody     dfv1.AbstractVertex
		inputVertexName = c.Param("vertex")
	)
	pl, err := h.numaflowClient.Pipelines(c.Param("namespace")).Get(context.Background(), c.Param("pipeline"), metav1.GetOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: %v", c.Param("namespace"), c.Param("pipeline"), inputVertexName, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	err = json.NewDecoder(c.Request.Body).Decode(&requestBody)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: %v", c.Param("namespace"), c.Param("pipeline"), inputVertexName, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	if requestBody.Name != inputVertexName {
		errMsg := fmt.Sprintf("Failed to update the vertex: vertex name %q is immutable", inputVertexName)
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	for index, vertex := range pl.Spec.Vertices {
		if vertex.Name == inputVertexName {
			if vertex.IsASource() && requestBody.IsASource() {
			} else if vertex.IsMapUDF() && requestBody.IsMapUDF() {
			} else if vertex.IsReduceUDF() && requestBody.IsReduceUDF() {
			} else if vertex.IsASink() && requestBody.IsASink() {
			} else if vertex.IsUDSource() && requestBody.IsUDSource() {
			} else if vertex.IsUDSource() && requestBody.IsUDSink() {
			} else {
				errMsg := fmt.Sprintf("Failed to update the vertex: vertex type is immutable")
				c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
				return
			}
			pl.Spec.Vertices[index] = requestBody
			break
		}
	}
	_, err = h.numaflowClient.Pipelines(c.Param("namespace")).Update(context.Background(), pl, metav1.UpdateOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: %v", c.Param("namespace"), c.Param("pipeline"), inputVertexName, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, pl.Spec))
}

// GetVerticesMetrics is used to provide information about all the vertices for the given pipeline including processing rates.
func (h *handler) GetVerticesMetrics(c *gin.Context) {
	ns := c.Param("namespace")
	pipeline := c.Param("pipeline")
	pl, err := h.numaflowClient.Pipelines(ns).Get(context.Background(), pipeline, metav1.GetOptions{})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get the vertices metrics: namespace %q pipeline %q: %v", ns, pipeline, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	client, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get the vertices metrics: failed to get demon service client for namespace %q pipeline %q: %v", ns, pipeline, err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	defer func() {
		_ = client.Close()
	}()
	var results [][]*daemon.VertexMetrics
	for _, vertex := range pl.Spec.Vertices {
		l, err := client.GetVertexMetrics(context.Background(), pipeline, vertex.Name)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to get the vertices metrics: namespace %q pipeline %q vertex %q: %v", c.Param("namespace"), c.Param("pipeline"), vertex.Name, err.Error())
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
			return
		}
		results = append(results, l)
	}
	c.JSON(http.StatusOK, results)
}

// ListVertexPods is used to provide all the pods of a vertex
func (h *handler) ListVertexPods(c *gin.Context) {
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	pods, err := h.kubeClient.CoreV1().Pods(c.Param("namespace")).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, c.Param("pipeline"), dfv1.KeyVertexName, c.Param("vertex")),
		Limit:         limit,
		Continue:      c.Query("continue"),
	})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get a list of pods: namespace %q pipeline %q vertex %q: %v", c.Param("namespace"), c.Param("pipeline"), c.Param("vertex"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, pods.Items))
}

// ListPodsMetrics is used to provide a list of all metrics in all the pods
func (h *handler) ListPodsMetrics(c *gin.Context) {
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	l, err := h.metricsClient.MetricsV1beta1().PodMetricses(c.Param("namespace")).List(context.Background(), metav1.ListOptions{
		Limit:    limit,
		Continue: c.Query("continue"),
	})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get a list of pod metrics in namespace %q: %v", c.Param("namespace"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, l.Items))
}

// PodLogs is used to provide the logs of a given container in pod
func (h *handler) PodLogs(c *gin.Context) {
	var tailLines *int64
	if v := c.Query("tailLines"); v != "" {
		x, _ := strconv.ParseInt(v, 10, 64)
		tailLines = pointer.Int64(x)
	}
	stream, err := h.kubeClient.CoreV1().
		Pods(c.Param("namespace")).
		GetLogs(c.Param("pod"), &corev1.PodLogOptions{
			Container: c.Query("container"),
			Follow:    c.Query("follow") == "true",
			TailLines: tailLines,
		}).Stream(c)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get pod logs: %v", err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	defer stream.Close()
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		_, _ = c.Writer.Write(scanner.Bytes())
		_, _ = c.Writer.WriteString("\n")
		c.Writer.Flush()
	}
}

// GetNamespaceEvents gets a list of events for the given namespace.
func (h *handler) GetNamespaceEvents(c *gin.Context) {
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	events, err := h.kubeClient.CoreV1().Events(c.Param("namespace")).List(context.Background(), metav1.ListOptions{
		Limit:    limit,
		Continue: c.Query("continue"),
	})
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get a list of events: namespace %q: %v", c.Param("namespace"), err.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, events.Items))
}

// ValidatePipeline is used to validate the pipeline spec
func (h *handler) ValidatePipeline(c *gin.Context) {
	reqBody, err := parseSpecFromReq(c, SpecTypePipeline)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body, %v", err.Error())
		c.JSON(http.StatusOK, errMsg)
		return
	}
	// Convert reqBody to pipeline spec
	var pipelineSpec = reqBody.(*dfv1.Pipeline)
	isValid := validatePipelineSpec(h, pipelineSpec)
	if isValid != nil {
		errMsg := fmt.Sprintf("Failed to validate pipeline spec, %v", isValid.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

func (h *handler) ValidateInterStepBufferService(c *gin.Context) {
	reqBody, err := parseSpecFromReq(c, SpecTypeISB)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to parse request body, %v", err.Error())
		c.JSON(http.StatusOK, errMsg)
		return
	}
	// Convert reqBody to pipeline spec
	var isbSpec = reqBody.(*dfv1.InterStepBufferService)
	isValid := validateISBSpec(h, isbSpec)
	if isValid != nil {
		errMsg := fmt.Sprintf("Failed to validate interstepbuffer service spec, %v", isValid.Error())
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
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
		resp := NewPipelineInfo(status, &pl)
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
		//	errMsg := fmt.Sprintf("Failed to fetch interstepbuffer service %q namespace %q, %v", isb.Name, isb.Namespace, err.Error())
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

// validatePipelineSpec is used to validate the pipeline spec
func validatePipelineSpec(h *handler, pipeline *dfv1.Pipeline) error {
	ns := pipeline.Namespace
	pipeClient := h.numaflowClient.Pipelines(ns)
	valid := validator.NewPipelineValidator(h.kubeClient, pipeClient, nil, pipeline)
	resp := valid.ValidateCreate(context.Background())
	if !resp.Allowed {
		errMsg := fmt.Errorf("%v", resp.Result.Message)
		return errMsg
	}
	return nil
}

// validateISBSpec is used to validate the ISB service spec
func validateISBSpec(h *handler, isb *dfv1.InterStepBufferService) error {
	ns := isb.Namespace
	isbClient := h.numaflowClient.InterStepBufferServices(ns)
	valid := validator.NewISBServiceValidator(h.kubeClient, isbClient, nil, isb)
	resp := valid.ValidateCreate(context.Background())
	if !resp.Allowed {
		errMsg := fmt.Errorf("%v", resp.Result.Message)
		return errMsg
	}
	return nil
}

func daemonSvcAddress(ns, pipeline string) string {
	return fmt.Sprintf("%s.%s.svc:%d", fmt.Sprintf("%s-daemon-svc", pipeline), ns, dfv1.DaemonServicePort)
}
