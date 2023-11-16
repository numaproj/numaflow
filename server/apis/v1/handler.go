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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	lru "github.com/hashicorp/golang-lru"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsversiond "k8s.io/metrics/pkg/client/clientset/versioned"
	"k8s.io/utils/pointer"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	dfv1versiond "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/common"
	"github.com/numaproj/numaflow/webhook/validator"
)

// Constants for the validation of the pipeline
const (
	ValidTypeCreate = "valid-create"
	ValidTypeUpdate = "valid-update"
)

type handler struct {
	kubeClient         kubernetes.Interface
	metricsClient      *metricsversiond.Clientset
	numaflowClient     dfv1clients.NumaflowV1alpha1Interface
	daemonClientsCache *lru.Cache
	dexObj             *DexObject
}

// NewHandler is used to provide a new instance of the handler type
func NewHandler(dexObj *DexObject) (*handler, error) {
	var (
		k8sRestConfig *rest.Config
		err           error
	)
	k8sRestConfig, err = util.K8sRestConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeRestConfig, %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(k8sRestConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeclient, %w", err)
	}
	metricsClient := metricsversiond.NewForConfigOrDie(k8sRestConfig)
	numaflowClient := dfv1versiond.NewForConfigOrDie(k8sRestConfig).NumaflowV1alpha1()
	daemonClientsCache, _ := lru.NewWithEvict(100, func(key, value interface{}) {
		_ = value.(*daemonclient.DaemonClient).Close()
	})
	return &handler{
		kubeClient:         kubeClient,
		metricsClient:      metricsClient,
		numaflowClient:     numaflowClient,
		daemonClientsCache: daemonClientsCache,
		dexObj:             dexObj,
	}, nil
}

// AuthInfo loads and returns auth info from cookie
func (h *handler) AuthInfo(c *gin.Context) {
	if h.dexObj == nil {
		errMsg := "User is not authenticated: missing Dex"
		c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
	}
	cookies := c.Request.Cookies()
	userIdentityTokenStr, err := common.JoinCookies(common.UserIdentityCookieName, cookies)
	if err != nil {
		errMsg := fmt.Sprintf("User is not authenticated, err: %s", err.Error())
		c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	if userIdentityTokenStr == "" {
		errMsg := "User is not authenticated, err: empty Token"
		c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	var userInfo authn.UserInfo
	if err = json.Unmarshal([]byte(userIdentityTokenStr), &userInfo); err != nil {
		errMsg := fmt.Sprintf("User is not authenticated, err: %s", err.Error())
		c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	idToken, err := h.dexObj.verify(c.Request.Context(), userInfo.IDToken)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to verify ID token: %s", err)
		c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}
	var claims authn.IDTokenClaims
	if err = idToken.Claims(&claims); err != nil {
		errMsg := fmt.Sprintf("Error decoding ID token claims: %s", err)
		c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	res := authn.NewUserInfo(&claims, userInfo.IDToken, userInfo.RefreshToken)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, res))
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

// GetClusterSummary summarizes information of all the namespaces in a cluster except the kube system namespaces
// and wrapped the result in a list.
func (h *handler) GetClusterSummary(c *gin.Context) {
	namespaces, err := getAllNamespaces(h)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch all namespaces, %s", err.Error()))
		return
	}

	type namespaceSummary struct {
		pipelineSummary PipelineSummary
		isbsvcSummary   IsbServiceSummary
	}
	var namespaceSummaryMap = make(map[string]namespaceSummary)

	// get pipeline summary
	pipelineList, err := h.numaflowClient.Pipelines("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch cluster summary, %s", err.Error()))
		return
	}
	for _, pipeline := range pipelineList.Items {
		var summary namespaceSummary
		if value, ok := namespaceSummaryMap[pipeline.Namespace]; ok {
			summary = value
		}
		status, err := getPipelineStatus(&pipeline)
		if err != nil {
			h.respondWithError(c, fmt.Sprintf("Failed to fetch cluster summary, %s", err.Error()))
			return
		}
		if status == PipelineStatusInactive {
			summary.pipelineSummary.Inactive++
		} else {
			summary.pipelineSummary.Active.increment(status)
		}
		namespaceSummaryMap[pipeline.Namespace] = summary
	}

	// get isbsvc summary
	isbsvcList, err := h.numaflowClient.InterStepBufferServices("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch cluster summary, %s", err.Error()))
		return
	}
	for _, isbsvc := range isbsvcList.Items {
		var summary namespaceSummary
		if value, ok := namespaceSummaryMap[isbsvc.Namespace]; ok {
			summary = value
		}
		status, err := getIsbServiceStatus(&isbsvc)
		if err != nil {
			h.respondWithError(c, fmt.Sprintf("Failed to fetch cluster summary, %s", err.Error()))
			return
		}
		if status == ISBServiceStatusInactive {
			summary.isbsvcSummary.Inactive++
		} else {
			summary.isbsvcSummary.Active.increment(status)
		}
		namespaceSummaryMap[isbsvc.Namespace] = summary
	}

	// get cluster summary
	var clusterSummary ClusterSummaryResponse
	// at this moment, if a namespace has neither pipeline nor isbsvc, it will not be included in the namespacedSummaryMap.
	// since we still want to pass these empty namespaces to the frontend, we add them here.
	for _, ns := range namespaces {
		if _, ok := namespaceSummaryMap[ns]; !ok {
			// if the namespace is not in the namespaceSummaryMap, it means it has neither pipeline nor isbsvc
			// taking advantage of golang by default initializing the struct with zero value
			namespaceSummaryMap[ns] = namespaceSummary{}
		}
	}
	for name, summary := range namespaceSummaryMap {
		clusterSummary = append(clusterSummary, NewNamespaceSummary(name, summary.pipelineSummary, summary.isbsvcSummary))
	}

	// sort the cluster summary by namespace in alphabetical order,
	// such that for first-time user, they get to see the default namespace first hence know where to start
	sort.Slice(clusterSummary, func(i, j int) bool {
		return clusterSummary[i].Namespace < clusterSummary[j].Namespace
	})

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, clusterSummary))
}

// CreatePipeline is used to create a given pipeline
func (h *handler) CreatePipeline(c *gin.Context) {
	ns := c.Param("namespace")
	// dryRun is used to check if the operation is just a validation or an actual creation
	dryRun := strings.EqualFold("true", c.DefaultQuery("dry-run", "false"))

	var pipelineSpec dfv1.Pipeline
	if err := bindJson(c, &pipelineSpec); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to decode JSON request body to pipeline spec, %s", err.Error()))
		return
	}

	if requestedNs := pipelineSpec.Namespace; !isValidNamespaceSpec(requestedNs, ns) {
		h.respondWithError(c, fmt.Sprintf("namespace mismatch, expected %s, got %s", ns, requestedNs))
		return
	}
	pipelineSpec.Namespace = ns

	if err := validatePipelineSpec(h, nil, &pipelineSpec, ValidTypeCreate); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to validate pipeline spec, %s", err.Error()))
		return
	}
	// if the validation flag "dryRun" is set to true, return without creating the pipeline
	if dryRun {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}
	if _, err := h.numaflowClient.Pipelines(ns).Create(context.Background(), &pipelineSpec, metav1.CreateOptions{}); err != nil {
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
	// set pl kind and apiVersion
	pl.Kind = dfv1.PipelineGroupVersionKind.Kind
	pl.APIVersion = dfv1.SchemeGroupVersion.String()

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
	client, err := h.getDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}

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
	// set the lag to be -1
	if minWM == -1 {
		lag = -1
	} else {
		lag = maxWM - minWM
	}

	pipelineResp := NewPipelineInfo(status, &lag, pl)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, pipelineResp))
}

// UpdatePipeline is used to update a given pipeline
func (h *handler) UpdatePipeline(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")
	// dryRun is used to check if the operation is just a validation or an actual update
	dryRun := strings.EqualFold("true", c.DefaultQuery("dry-run", "false"))

	oldSpec, err := h.numaflowClient.Pipelines(ns).Get(context.Background(), pipeline, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch pipeline %q namespace %q, %s", pipeline, ns, err.Error()))
		return
	}

	var updatedSpec dfv1.Pipeline
	if err = bindJson(c, &updatedSpec); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to decode JSON request body to pipeline spec, %s", err.Error()))
		return
	}

	if requestedNs := updatedSpec.Namespace; !isValidNamespaceSpec(requestedNs, ns) {
		h.respondWithError(c, fmt.Sprintf("namespace mismatch, expected %s, got %s", ns, requestedNs))
		return
	}
	updatedSpec.Namespace = ns

	// pipeline name in the URL should be the same as spec name
	if pipeline != updatedSpec.Name {
		h.respondWithError(c, fmt.Sprintf("pipeline name %q is immutable", pipeline))
		return
	}

	isValid := validatePipelineSpec(h, oldSpec, &updatedSpec, ValidTypeUpdate)
	if isValid != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update pipeline %q, %s", pipeline, isValid.Error()))
		return
	}
	// If the validation flag is set to true, return without updating the pipeline
	if dryRun {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}

	oldSpec.Spec = updatedSpec.Spec
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

	// cleanup client after successfully deleting pipeline
	h.daemonClientsCache.Remove(daemonSvcAddress(ns, pipeline))

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// PatchPipeline is used to patch the pipeline spec to achieve operations such as "pause" and "resume"
func (h *handler) PatchPipeline(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	patchSpec, err := io.ReadAll(c.Request.Body)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to parse request body, %s", err.Error()))
		return
	}

	err = validatePipelinePatch(patchSpec)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to patch pipeline %q, %s", pipeline, err.Error()))
		return
	}

	if _, err := h.numaflowClient.Pipelines(ns).Patch(context.Background(), pipeline, types.MergePatchType, patchSpec, metav1.PatchOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to patch pipeline %q, %s", pipeline, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

func (h *handler) CreateInterStepBufferService(c *gin.Context) {
	ns := c.Param("namespace")
	// dryRun is used to check if the operation is just a validation or an actual update
	dryRun := strings.EqualFold("true", c.DefaultQuery("dry-run", "false"))

	var isbsvcSpec dfv1.InterStepBufferService
	if err := bindJson(c, &isbsvcSpec); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to decode JSON request body to interstepbuffer service spec, %s", err.Error()))
		return
	}

	if isbsvcNs := isbsvcSpec.Namespace; !isValidNamespaceSpec(isbsvcNs, ns) {
		h.respondWithError(c, fmt.Sprintf("namespace mismatch, expected %s, got %s", ns, isbsvcNs))
		return
	}
	isbsvcSpec.Namespace = ns

	isValid := validateISBSVCSpec(nil, &isbsvcSpec, ValidTypeCreate)
	if isValid != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to create interstepbuffer service spec, %s", isValid.Error()))
		return
	}
	// If the validation flag is set to true, return without creating the ISB service
	if dryRun {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}

	if _, err := h.numaflowClient.InterStepBufferServices(ns).Create(context.Background(), &isbsvcSpec, metav1.CreateOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to create interstepbuffer service %q, %s", isbsvcSpec.Name, err.Error()))
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
	ns, isbsvcName := c.Param("namespace"), c.Param("isb-service")

	isbsvc, err := h.numaflowClient.InterStepBufferServices(ns).Get(context.Background(), isbsvcName, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch interstepbuffer service %q namespace %q, %s", isbsvcName, ns, err.Error()))
		return
	}
	isbsvc.Kind = dfv1.ISBGroupVersionKind.Kind
	isbsvc.APIVersion = dfv1.SchemeGroupVersion.Version

	status, err := getIsbServiceStatus(isbsvc)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch interstepbuffer service %q namespace %q, %s", isbsvcName, ns, err.Error()))
	}

	resp := NewISBService(status, isbsvc)

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, resp))
}

func (h *handler) UpdateInterStepBufferService(c *gin.Context) {
	ns, isbsvcName := c.Param("namespace"), c.Param("isb-service")
	// dryRun is used to check if the operation is just a validation or an actual update
	dryRun := strings.EqualFold("true", c.DefaultQuery("dry-run", "false"))

	isbSVC, err := h.numaflowClient.InterStepBufferServices(ns).Get(context.Background(), isbsvcName, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the interstep buffer service: namespace %q isb-services %q: %s", ns, isbsvcName, err.Error()))
		return
	}

	var updatedSpec dfv1.InterStepBufferService
	if err = bindJson(c, &updatedSpec); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to decode JSON request body to interstepbuffer service spec, %s", err.Error()))
		return
	}

	if err = validateISBSVCSpec(isbSVC, &updatedSpec, ValidTypeUpdate); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to validate interstepbuffer service spec, %s", err.Error()))
		return
	}

	// If the validation flag is set to true, return without updating the ISB service
	if dryRun {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}
	isbSVC.Spec = updatedSpec.Spec
	updatedISBSvc, err := h.numaflowClient.InterStepBufferServices(ns).Update(context.Background(), isbSVC, metav1.UpdateOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update the interstep buffer service: namespace %q isb-services %q: %s", ns, isbsvcName, err.Error()))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, updatedISBSvc))
}

func (h *handler) DeleteInterStepBufferService(c *gin.Context) {
	ns, isbsvcName := c.Param("namespace"), c.Param("isb-service")

	pipelines, err := h.numaflowClient.Pipelines(ns).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get pipelines in namespace %q, %s", ns, err.Error()))
		return
	}
	for _, pl := range pipelines.Items {
		plISBSvcName := pl.Spec.InterStepBufferServiceName
		if (plISBSvcName == "" && isbsvcName == dfv1.DefaultISBSvcName) || (plISBSvcName == isbsvcName) {
			h.respondWithError(c, fmt.Sprintf("Failed to delete the interstep buffer service %q: this ISBSVC is in use by pipeline %s", isbsvcName, pl.Name))
			return
		}
	}

	err = h.numaflowClient.InterStepBufferServices(ns).Delete(context.Background(), isbsvcName, metav1.DeleteOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to delete the interstep buffer service: namespace %q isb-service %q: %s",
			ns, isbsvcName, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// ListPipelineBuffers is used to provide buffer information about all the pipeline vertices
func (h *handler) ListPipelineBuffers(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	client, err := h.getDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}

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

	client, err := h.getDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}

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
		// dryRun is used to check if the operation is just a validation or an actual creation
		dryRun = strings.EqualFold("true", c.DefaultQuery("dry-run", "false"))
	)

	pl, err := h.numaflowClient.Pipelines(ns).Get(context.Background(), pipeline, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: %s", ns,
			pipeline, inputVertexName, err.Error()))
		return
	}

	if err = bindJson(c, &requestBody); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to decode JSON request body to vertex spec, %s", err.Error()))
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

	err = validatePipelineSpec(h, nil, pl, ValidTypeCreate)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to validate pipeline spec, %s", err.Error()))
		return
	}
	// if the validation flag "dryRun" is set to true, return without creating the pipeline
	if dryRun {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
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

	client, err := h.getDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}

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
// It supports filtering by object type and object name.
// If objectType and objectName are specified in the request, only the events that match both will be returned.
// If objectType and objectName are not specified, all the events for the given name space will be returned.
// Events are sorted by timestamp in descending order.
func (h *handler) GetNamespaceEvents(c *gin.Context) {
	ns := c.Param("namespace")
	objType := c.DefaultQuery("objectType", "")
	objName := c.DefaultQuery("objectName", "")
	if (objType == "" && objName != "") || (objType != "" && objName == "") {
		h.respondWithError(c, fmt.Sprintf("Failed to get a list of events: namespace %q: "+
			"please either specify both objectType and objectName or not specify.", ns))
		return
	}
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	var err error
	var events *corev1.EventList
	if events, err = h.kubeClient.CoreV1().Events(ns).List(context.Background(), metav1.ListOptions{
		Limit:    limit,
		Continue: c.Query("continue"),
	}); err != nil {
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
		if (objType == "" && objName == "") ||
			(strings.EqualFold(event.InvolvedObject.Kind, objType) && strings.EqualFold(event.InvolvedObject.Name, objName)) {
			newEvent := NewK8sEventsResponse(event.LastTimestamp.UnixMilli(), event.Type, event.InvolvedObject.Kind, event.InvolvedObject.Name, event.Reason, event.Message)
			response = append(response, newEvent)
		}
	}
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
// except the kube system namespaces
func getAllNamespaces(h *handler) ([]string, error) {
	namespaces, err := h.kubeClient.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var res []string
	for _, ns := range namespaces.Items {
		// skip kube system namespaces because users are not supposed to create pipelines in them
		// see https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/#initial-namespaces
		if name := ns.Name; name != metav1.NamespaceSystem && name != metav1.NamespacePublic && name != "kube-node-lease" {
			res = append(res, ns.Name)
		}
	}
	return res, nil
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
		status, err := getIsbServiceStatus(&isb)
		if err != nil {
			return nil, err
		}
		resp := NewISBService(status, &isb)
		isbList = append(isbList, resp)
	}
	return isbList, nil
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
	return retStatus, nil
}

// GetIsbServiceStatus is used to provide the status of a given InterStepBufferService
// TODO: Figure out the correct way to determine if a ISBService is healthy
func getIsbServiceStatus(isbsvc *dfv1.InterStepBufferService) (string, error) {
	retStatus := ISBServiceStatusHealthy
	if isbsvc.Status.Phase == dfv1.ISBSvcPhaseUnknown {
		retStatus = ISBServiceStatusInactive
	} else if isbsvc.Status.Phase == dfv1.ISBSvcPhasePending || isbsvc.Status.Phase == dfv1.ISBSvcPhaseRunning {
		retStatus = ISBServiceStatusHealthy
	} else if isbsvc.Status.Phase == dfv1.ISBSvcPhaseFailed {
		retStatus = ISBServiceStatusCritical
	}
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
func validateISBSVCSpec(prevSpec *dfv1.InterStepBufferService,
	newSpec *dfv1.InterStepBufferService, validType string) error {
	// UI-specific validations: updating namespace and name from UI is not allowed
	if validType == ValidTypeUpdate && prevSpec != nil {
		if !isValidNamespaceSpec(newSpec.Namespace, prevSpec.Namespace) {
			return fmt.Errorf("updating an inter-step buffer service's namespace is not allowed, expected %s, got %s", prevSpec.Namespace, newSpec.Namespace)
		}
		if prevSpec.Name != newSpec.Name {
			return fmt.Errorf("updating an inter-step buffer service's name is not allowed, expected %s, got %s", prevSpec.Name, newSpec.Name)
		}
	}

	// the rest of the code leverages the webhook validator to validate the ISB service spec.
	valid := validator.NewISBServiceValidator(prevSpec, newSpec)
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

// isValidNamespaceSpec validates
// that the requested namespace should be either empty or the same as the current namespace
func isValidNamespaceSpec(requested, current string) bool {
	return requested == "" || requested == current
}

// bindJson is used to bind the request body to a given object
// It also validates the request body to ensure that it does not contain any unknown fields
func bindJson(c *gin.Context, obj interface{}) error {
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(obj); err != nil {
		return err
	}
	return nil
}

// validatePipelinePatch is used to validate the patch for a pipeline
func validatePipelinePatch(patch []byte) error {

	var patchSpec dfv1.Pipeline
	var emptySpec dfv1.Pipeline

	// check that patch is correctly formatted for pipeline
	if err := json.Unmarshal(patch, &patchSpec); err != nil {
		return err
	}

	// compare patch to empty pipeline spec to check that only lifecycle is being patched
	patchSpec.Spec.Lifecycle = dfv1.Lifecycle{}
	if !reflect.DeepEqual(patchSpec, emptySpec) {
		return fmt.Errorf("only spec.lifecycle is allowed for patching")
	}

	return nil
}

func daemonSvcAddress(ns, pipeline string) string {
	return fmt.Sprintf("%s.%s.svc:%d", fmt.Sprintf("%s-daemon-svc", pipeline), ns, dfv1.DaemonServicePort)
}

func (h *handler) getDaemonClient(ns, pipeline string) (*daemonclient.DaemonClient, error) {
	dClient, ok := h.daemonClientsCache.Get(daemonSvcAddress(ns, pipeline))
	if !ok {
		dClient, err := daemonclient.NewDaemonServiceClient(daemonSvcAddress(ns, pipeline))
		if err != nil {
			return nil, err
		}
		if dClient == nil {
			return nil, fmt.Errorf("nil client")
		}
		h.daemonClientsCache.Add(daemonSvcAddress(ns, pipeline), dClient)
	}

	client, ok := dClient.(*daemonclient.DaemonClient)
	if !ok {
		return nil, fmt.Errorf("failed to get client")
	}

	return client, nil
}
