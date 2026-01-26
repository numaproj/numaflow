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
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	lru "github.com/hashicorp/golang-lru/v2"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metricsclientv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"k8s.io/utils/ptr"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	dfv1versiond "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtdaemonclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/pkg/webhook/validator"
	"github.com/numaproj/numaflow/server/authn"
	"github.com/numaproj/numaflow/server/common"
)

// Constants for the validation of the pipeline
const (
	ValidTypeCreate = "valid-create"
	ValidTypeUpdate = "valid-update"
)

type handlerOptions struct {
	// readonly is used to indicate whether the server is in read-only mode
	readonly bool
	// daemonClientProtocol is used to indicate the protocol of the daemon client, 'grpc' or 'http'
	daemonClientProtocol string
}

func defaultHandlerOptions() *handlerOptions {
	return &handlerOptions{
		readonly:             false,
		daemonClientProtocol: "grpc",
	}
}

type HandlerOption func(*handlerOptions)

// WithDaemonClientProtocol sets the protocol of the daemon client.
func WithDaemonClientProtocol(protocol string) HandlerOption {
	return func(o *handlerOptions) {
		o.daemonClientProtocol = protocol
	}
}

// WithReadOnlyMode sets the server to read-only mode.
func WithReadOnlyMode() HandlerOption {
	return func(o *handlerOptions) {
		o.readonly = true
	}
}

type handler struct {
	kubeClient            kubernetes.Interface
	metricsClient         metricsclientv1beta1.MetricsV1beta1Interface
	promQlServiceObj      PromQl
	numaflowClient        dfv1clients.NumaflowV1alpha1Interface
	daemonClientsCache    *lru.Cache[string, daemonclient.DaemonClient]
	mvtDaemonClientsCache *lru.Cache[string, mvtdaemonclient.MonoVertexDaemonClient]
	dexObj                *DexObject
	localUsersAuthObject  *LocalUsersAuthObject
	healthChecker         *HealthChecker
	opts                  *handlerOptions
}

// NewHandler is used to provide a new instance of the handler type
func NewHandler(ctx context.Context, dexObj *DexObject, localUsersAuthObject *LocalUsersAuthObject, promQlServiceObj PromQl, opts ...HandlerOption) (*handler, error) {
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
	metricsClient := metricsclientv1beta1.NewForConfigOrDie(k8sRestConfig)
	numaflowClient := dfv1versiond.NewForConfigOrDie(k8sRestConfig).NumaflowV1alpha1()
	daemonClientsCache, _ := lru.NewWithEvict[string, daemonclient.DaemonClient](500, func(key string, value daemonclient.DaemonClient) {
		_ = value.Close()
	})
	mvtDaemonClientsCache, _ := lru.NewWithEvict[string, mvtdaemonclient.MonoVertexDaemonClient](500, func(key string, value mvtdaemonclient.MonoVertexDaemonClient) {
		_ = value.Close()
	})

	o := defaultHandlerOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(o)
		}
	}
	return &handler{
		kubeClient:            kubeClient,
		metricsClient:         metricsClient,
		promQlServiceObj:      promQlServiceObj,
		numaflowClient:        numaflowClient,
		daemonClientsCache:    daemonClientsCache,
		mvtDaemonClientsCache: mvtDaemonClientsCache,
		dexObj:                dexObj,
		localUsersAuthObject:  localUsersAuthObject,
		healthChecker:         NewHealthChecker(ctx),
		opts:                  o,
	}, nil
}

// AuthInfo loads and returns auth info from cookie
func (h *handler) AuthInfo(c *gin.Context) {
	if h.dexObj == nil && h.localUsersAuthObject == nil {
		errMsg := "User is not authenticated: missing Dex and LocalAuth"
		c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	loginType, err := c.Cookie(common.LoginCookieName)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to get login type: %v", err)
		c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	if loginType == "dex" {
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
		return
	} else if loginType == "local" {
		userIdentityTokenStr, err := c.Cookie(common.JWTCookieName)
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
		claims, err := h.localUsersAuthObject.ParseToken(c, userIdentityTokenStr)
		if err != nil {
			errMsg := fmt.Sprintf("Failed to verify and parse token: %s", err)
			c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
			return
		}

		itc := authn.IDTokenClaims{
			Iss:  claims["iss"].(string),
			Exp:  int(claims["exp"].(float64)),
			Iat:  int(claims["iat"].(float64)),
			Name: claims["username"].(string),
		}
		res := authn.NewUserInfo(&itc, userIdentityTokenStr, "")
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, res))
		return
	}

	errMsg := fmt.Sprintf("Unidentified login type received: %v", loginType)
	c.JSON(http.StatusUnauthorized, NewNumaflowAPIResponse(&errMsg, nil))
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
		pipelineSummary   PipelineSummary
		isbsvcSummary     IsbServiceSummary
		monoVertexSummary MonoVertexSummary
	}
	var namespaceSummaryMap = make(map[string]namespaceSummary)

	// get pipeline summary
	pipelineList, err := h.numaflowClient.Pipelines("").List(c, metav1.ListOptions{})
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
		if status == dfv1.PipelineStatusInactive {
			summary.pipelineSummary.Inactive++
		} else {
			summary.pipelineSummary.Active.increment(status)
		}
		namespaceSummaryMap[pipeline.Namespace] = summary
	}

	// get isbsvc summary
	isbsvcList, err := h.numaflowClient.InterStepBufferServices("").List(c, metav1.ListOptions{})
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

	// get mono vertex summary
	mvtList, err := h.numaflowClient.MonoVertices("").List(c, metav1.ListOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch cluster summary, failed to fetch mono vertex list, %s", err.Error()))
		return
	}
	for _, monoVertex := range mvtList.Items {
		var summary namespaceSummary
		if value, ok := namespaceSummaryMap[monoVertex.Namespace]; ok {
			summary = value
		}
		status, err := getMonoVertexStatus(&monoVertex)
		if err != nil {
			h.respondWithError(c, fmt.Sprintf("Failed to fetch cluster summary, failed to get the status of the mono vertex %s, %s", monoVertex.Name, err.Error()))
			return
		}
		// if the mono vertex is healthy, increment the active count, otherwise increment the inactive count
		// TODO - add more status types for mono vertex and update the logic here
		if status == dfv1.MonoVertexStatusHealthy {
			summary.monoVertexSummary.Active.increment(status)
		} else {
			summary.monoVertexSummary.Inactive++
		}
		namespaceSummaryMap[monoVertex.Namespace] = summary
	}

	// get cluster summary
	var clusterSummary ClusterSummaryResponse
	// at this moment, if a namespace has neither pipeline nor isbsvc, it will not be included in the namespacedSummaryMap.
	// since we still want to pass these empty namespaces to the frontend, we add them here.
	for _, ns := range namespaces {
		if _, ok := namespaceSummaryMap[ns]; !ok {
			// if the namespace is not in the namespaceSummaryMap, it means it has none of the pipelines, isbsvc, or mono vertex
			// taking advantage of golang by default initializing the struct with zero value
			namespaceSummaryMap[ns] = namespaceSummary{}
		}
	}
	for name, summary := range namespaceSummaryMap {
		clusterSummary = append(clusterSummary, NewNamespaceSummary(name, summary.pipelineSummary, summary.isbsvcSummary, summary.monoVertexSummary))
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
	if h.opts.readonly {
		errMsg := "Failed to perform this operation in read only mode"
		c.JSON(http.StatusForbidden, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

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
	if _, err := h.numaflowClient.Pipelines(ns).Create(c, &pipelineSpec, metav1.CreateOptions{}); err != nil {
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
	pl, err := h.numaflowClient.Pipelines(ns).Get(c, pipeline, metav1.GetOptions{})
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
	client, err := h.getPipelineDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}

	var (
		minWM int64 = math.MaxInt64
		maxWM int64 = math.MinInt64
	)
	watermarks, err := client.GetPipelineWatermarks(c, pipeline)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch pipeline: failed to calculate lag for pipeline %q: %s", pipeline, err.Error()))
		return
	}
	for _, watermark := range watermarks {
		// find the largest source vertex watermark
		if _, ok := source[watermark.From]; ok {
			for _, wm := range watermark.Watermarks {
				if wm.GetValue() > maxWM {
					maxWM = wm.GetValue()
				}
			}
		}
		// find the smallest sink vertex watermark
		if _, ok := sink[watermark.To]; ok {
			for _, wm := range watermark.Watermarks {
				if wm.GetValue() < minWM {
					minWM = wm.GetValue()
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
	if h.opts.readonly {
		errMsg := "Failed to perform this operation in read only mode"
		c.JSON(http.StatusForbidden, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	ns, pipeline := c.Param("namespace"), c.Param("pipeline")
	// dryRun is used to check if the operation is just a validation or an actual update
	dryRun := strings.EqualFold("true", c.DefaultQuery("dry-run", "false"))

	oldSpec, err := h.numaflowClient.Pipelines(ns).Get(c, pipeline, metav1.GetOptions{})
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
	if _, err := h.numaflowClient.Pipelines(ns).Update(c, oldSpec, metav1.UpdateOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update pipeline %q, %s", pipeline, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// DeletePipeline is used to delete a given pipeline
func (h *handler) DeletePipeline(c *gin.Context) {
	if h.opts.readonly {
		errMsg := "Failed to perform this operation in read only mode"
		c.JSON(http.StatusForbidden, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	if err := h.numaflowClient.Pipelines(ns).Delete(c, pipeline, metav1.DeleteOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to delete pipeline %q, %s", pipeline, err.Error()))
		return
	}

	// cleanup client after successfully deleting pipeline
	// NOTE: if a pipeline was deleted by not through UI, the cache will not be updated,
	// the entry becomes invalid and will be evicted only after the cache is full.
	h.daemonClientsCache.Remove(pipelineDaemonSvcAddress(ns, pipeline))

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// PatchPipeline is used to patch the pipeline spec to achieve operations such as "pause" and "resume"
func (h *handler) PatchPipeline(c *gin.Context) {
	if h.opts.readonly {
		errMsg := "Failed to perform this operation in read only mode"
		c.JSON(http.StatusForbidden, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

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

	if _, err := h.numaflowClient.Pipelines(ns).Patch(c, pipeline, types.MergePatchType, patchSpec, metav1.PatchOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to patch pipeline %q, %s", pipeline, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

func (h *handler) CreateInterStepBufferService(c *gin.Context) {
	if h.opts.readonly {
		errMsg := "Failed to perform this operation in read only mode"
		c.JSON(http.StatusForbidden, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

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

	if _, err := h.numaflowClient.InterStepBufferServices(ns).Create(c, &isbsvcSpec, metav1.CreateOptions{}); err != nil {
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

	isbsvc, err := h.numaflowClient.InterStepBufferServices(ns).Get(c, isbsvcName, metav1.GetOptions{})
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
	if h.opts.readonly {
		errMsg := "Failed to perform this operation in read only mode"
		c.JSON(http.StatusForbidden, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	ns, isbsvcName := c.Param("namespace"), c.Param("isb-service")
	// dryRun is used to check if the operation is just a validation or an actual update
	dryRun := strings.EqualFold("true", c.DefaultQuery("dry-run", "false"))

	isbSVC, err := h.numaflowClient.InterStepBufferServices(ns).Get(c, isbsvcName, metav1.GetOptions{})
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
	updatedISBSvc, err := h.numaflowClient.InterStepBufferServices(ns).Update(c, isbSVC, metav1.UpdateOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update the interstep buffer service: namespace %q isb-services %q: %s", ns, isbsvcName, err.Error()))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, updatedISBSvc))
}

func (h *handler) DeleteInterStepBufferService(c *gin.Context) {
	if h.opts.readonly {
		errMsg := "Failed to perform this operation in read only mode"
		c.JSON(http.StatusForbidden, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	ns, isbsvcName := c.Param("namespace"), c.Param("isb-service")

	pipelines, err := h.numaflowClient.Pipelines(ns).List(c, metav1.ListOptions{})
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

	err = h.numaflowClient.InterStepBufferServices(ns).Delete(c, isbsvcName, metav1.DeleteOptions{})
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

	client, err := h.getPipelineDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}

	buffers, err := client.ListPipelineBuffers(c, pipeline)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the Inter-Step buffers for pipeline %q: %s", pipeline, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, buffers))
}

// GetPipelineWatermarks is used to provide the head watermarks for a given pipeline
func (h *handler) GetPipelineWatermarks(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	client, err := h.getPipelineDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}

	watermarks, err := client.GetPipelineWatermarks(c, pipeline)
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
	if h.opts.readonly {
		errMsg := "Failed to perform this operation in read only mode"
		c.JSON(http.StatusForbidden, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	var (
		requestBody     dfv1.AbstractVertex
		inputVertexName = c.Param("vertex")
		pipeline        = c.Param("pipeline")
		ns              = c.Param("namespace")
		// dryRun is used to check if the operation is just a validation or an actual creation
		dryRun = strings.EqualFold("true", c.DefaultQuery("dry-run", "false"))
	)

	oldPipelineSpec, err := h.numaflowClient.Pipelines(ns).Get(c, pipeline, metav1.GetOptions{})
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

	newPipelineSpec := oldPipelineSpec.DeepCopy()
	for index, vertex := range newPipelineSpec.Spec.Vertices {
		if vertex.Name == inputVertexName {
			newPipelineSpec.Spec.Vertices[index] = requestBody
			break
		}
	}

	err = validatePipelineSpec(h, oldPipelineSpec, newPipelineSpec, ValidTypeUpdate)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to validate pipeline spec, %s", err.Error()))
		return
	}
	// if the validation flag "dryRun" is set to true, return without updating the pipeline
	if dryRun {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}

	oldPipelineSpec.Spec = newPipelineSpec.Spec
	if _, err := h.numaflowClient.Pipelines(ns).Update(c, oldPipelineSpec, metav1.UpdateOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to update the vertex: namespace %q pipeline %q vertex %q: %s",
			ns, pipeline, inputVertexName, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, oldPipelineSpec.Spec))
}

// GetVerticesMetrics is used to provide information about all the vertices for the given pipeline including processing rates.
func (h *handler) GetVerticesMetrics(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	pl, err := h.numaflowClient.Pipelines(ns).Get(c, pipeline, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the vertices metrics: namespace %q pipeline %q: %s", ns, pipeline, err.Error()))
		return
	}

	client, err := h.getPipelineDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}

	var results = make(map[string][]*daemon.VertexMetrics)
	for _, vertex := range pl.Spec.Vertices {
		metrics, err := client.GetVertexMetrics(c, pipeline, vertex.Name)
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
	pods, err := h.kubeClient.CoreV1().Pods(ns).List(c, metav1.ListOptions{
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
	metrics, err := h.metricsClient.PodMetricses(ns).List(c, metav1.ListOptions{
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
		Container:  c.Query("container"),
		Follow:     c.Query("follow") == "true",
		TailLines:  tailLines,
		Timestamps: true,
		Previous:   c.Query("previous") == "true",
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

func (h *handler) GetMonoVertexPodsInfo(c *gin.Context) {
	var response = make([]PodDetails, 0)
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")
	pods, err := h.kubeClient.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", dfv1.KeyMonoVertexName, monoVertex),
	})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("GetMonoVertexPodInfo: Failed to get a list of pods: namespace %q mono vertex %q: %s",
			ns, monoVertex, err.Error()))
		return
	}
	if pods == nil || len(pods.Items) == 0 {
		h.respondWithError(c, fmt.Sprintf("GetMonoVertexPodInfo: No pods found for mono vertex %q in namespace %q", monoVertex, ns))
		return
	}
	for _, pod := range pods.Items {
		podDetails, err := h.getPodDetails(pod)
		if err != nil {
			h.respondWithError(c, fmt.Sprintf("GetMonoVertexPodInfo: Failed to get the pod details: %v", err))
			return
		} else {
			response = append(response, podDetails)
		}
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, response))
}

func (h *handler) GetVertexPodsInfo(c *gin.Context) {
	var response = make([]PodDetails, 0)
	ns, pipeline, vertex := c.Param("namespace"), c.Param("pipeline"), c.Param("vertex")
	pods, err := h.kubeClient.CoreV1().Pods(ns).List(context.TODO(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s", dfv1.KeyPipelineName, pipeline, dfv1.KeyVertexName, vertex),
	})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("GetVertexPodsInfo: Failed to get a list of pods: namespace %q pipeline %q vertex %q: %s",
			ns, pipeline, vertex, err.Error()))
		return
	}
	if pods == nil || len(pods.Items) == 0 {
		h.respondWithError(c, fmt.Sprintf("GetVertexPodsInfo: No pods found for pipeline %q vertex %q in namespace %q", pipeline, vertex, ns))
		return
	}
	for _, pod := range pods.Items {
		podDetails, err := h.getPodDetails(pod)
		if err != nil {
			h.respondWithError(c, fmt.Sprintf("GetVertexPodsInfo: Failed to get the pod details: %v", err))
			return
		} else {
			response = append(response, podDetails)
		}
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, response))

}
func (h *handler) parseTailLines(query string) *int64 {
	if query == "" {
		return nil
	}

	x, _ := strconv.ParseInt(query, 10, 64)
	return ptr.To[int64](x)
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
	if events, err = h.kubeClient.CoreV1().Events(ns).List(c, metav1.ListOptions{
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

// GetPipelineStatus returns the pipeline status. It is based on Resource Health and Data Criticality.
// Resource Health can be "healthy (0) | unhealthy (1) | paused (3) | unknown (4)".
// A backlogged pipeline can be healthy even though it has an increasing back-pressure.
// Resource Health purely means it is up and running.
// Resource health will be the max(health) based of each vertex's health
// Data Criticality on the other end shows whether the pipeline is working as expected.
// It represents the pending messages, lags, etc.
// Data Criticality can be "ok (0) | warning (1) | critical (2)".
// GetPipelineStatus is used to return the status of a given pipeline
// It is divided into two parts:
// 1. Pipeline Resource Health: It is based on the health of each vertex in the pipeline
// 2. Data Criticality: It is based on the data movement of the pipeline
func (h *handler) GetPipelineStatus(c *gin.Context) {
	ns, pipeline := c.Param("namespace"), c.Param("pipeline")

	// Get the vertex level health of the pipeline
	resourceHealth, err := h.healthChecker.getPipelineResourceHealth(h, ns, pipeline)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the resourceHealth for pipeline %q: %s", pipeline, err.Error()))
		return
	}

	// Get a new daemon client for the given pipeline
	client, err := h.getPipelineDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}
	// Get the data criticality for the given pipeline
	dataStatus, err := client.GetPipelineStatus(c, pipeline)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the dataStatus for pipeline %q: %s", pipeline, err.Error()))
		return
	}

	// Create a response string based on the vertex health and data criticality
	// We combine both the states to get the final dataStatus of the pipeline
	response := NewHealthResponse(resourceHealth.Status, dataStatus.GetStatus(),
		resourceHealth.Message, dataStatus.GetMessage(), resourceHealth.Code, dataStatus.GetCode())

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, response))
}

// ListMonoVertices is used to provide all the mono vertices in a namespace.
func (h *handler) ListMonoVertices(c *gin.Context) {
	ns := c.Param("namespace")
	mvtList, err := getMonoVertices(h, ns)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch all mono vertices for namespace %q, %s", ns, err.Error()))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, mvtList))
}

// GetMonoVertex is used to provide the spec of a given mono vertex
func (h *handler) GetMonoVertex(c *gin.Context) {
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")
	// get general mono vertex info
	mvt, err := h.numaflowClient.MonoVertices(ns).Get(c, monoVertex, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch mono vertex %q in namespace %q, %s", mvt, ns, err.Error()))
		return
	}
	// set mono vertex kind and apiVersion
	mvt.Kind = dfv1.MonoVertexGroupVersionKind.Kind
	mvt.APIVersion = dfv1.SchemeGroupVersion.String()
	// get mono vertex status
	status, err := getMonoVertexStatus(mvt)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch mono vertex %q from namespace %q, %s", monoVertex, ns, err.Error()))
		return
	}
	monoVertexResp := NewMonoVertexInfo(status, mvt)
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, monoVertexResp))
}

// DeleteMonoVertex is used to delete a mono vertex
func (h *handler) DeleteMonoVertex(c *gin.Context) {
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")

	// Check if the mono vertex exists
	_, err := h.numaflowClient.MonoVertices(ns).Get(c, monoVertex, metav1.GetOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to fetch mono vertex %q in namespace %q, %s", monoVertex, ns, err.Error()))
		return
	}

	// Delete the mono vertex
	err = h.numaflowClient.MonoVertices(ns).Delete(c, monoVertex, metav1.DeleteOptions{})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to delete mono vertex %q in namespace %q, %s", monoVertex, ns, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// CreateMonoVertex is used to create a mono vertex
func (h *handler) CreateMonoVertex(c *gin.Context) {
	if h.opts.readonly {
		errMsg := "Failed to perform this operation in read only mode"
		c.JSON(http.StatusForbidden, NewNumaflowAPIResponse(&errMsg, nil))
		return
	}

	ns := c.Param("namespace")
	// dryRun is used to check if the operation is just a validation or an actual creation
	dryRun := strings.EqualFold("true", c.DefaultQuery("dry-run", "false"))

	var monoVertexSpec dfv1.MonoVertex
	if err := bindJson(c, &monoVertexSpec); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to decode JSON request body to mono vertex spec, %s", err.Error()))
		return
	}

	if requestedNs := monoVertexSpec.Namespace; !isValidNamespaceSpec(requestedNs, ns) {
		h.respondWithError(c, fmt.Sprintf("namespace mismatch, expected %s, got %s", ns, requestedNs))
		return
	}
	monoVertexSpec.Namespace = ns
	// if the validation flag "dryRun" is set to true, return without creating the pipeline
	if dryRun {
		c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
		return
	}
	if _, err := h.numaflowClient.MonoVertices(ns).Create(c, &monoVertexSpec, metav1.CreateOptions{}); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to create mono vertex %q, %s", monoVertexSpec.Name, err.Error()))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, nil))
}

// ListMonoVertexPods is used to provide all the pods of a mono vertex
func (h *handler) ListMonoVertexPods(c *gin.Context) {
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")
	limit, _ := strconv.ParseInt(c.Query("limit"), 10, 64)
	pods, err := h.kubeClient.CoreV1().Pods(ns).List(c, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", dfv1.KeyMonoVertexName, monoVertex),
		Limit:         limit,
		Continue:      c.Query("continue"),
	})
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get a list of pods: namespace %q mono vertex %q: %s",
			ns, monoVertex, err.Error()))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, pods.Items))
}

// GetMonoVertexMetrics is used to provide information about one mono vertex, including processing rates.
func (h *handler) GetMonoVertexMetrics(c *gin.Context) {
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")

	client, err := h.getMonoVertexDaemonClient(ns, monoVertex)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for mono vertex %q, %s", monoVertex, err.Error()))
		return
	}

	metrics, err := client.GetMonoVertexMetrics(c)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the mono vertex metrics: namespace %q mono vertex %q: %s", ns, monoVertex, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, metrics))
}

// GetMonoVertexHealth is used to the health information about a mono vertex
// We use two checks to determine the health of the mono vertex:
// 1. Resource Health: It is based on the health of the mono vertex deployment and pods.
// 2. Data Criticality: It is based on the data movement of the mono vertex
func (h *handler) GetMonoVertexHealth(c *gin.Context) {
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")

	// Resource level health
	resourceHealth, err := h.healthChecker.getMonoVtxResourceHealth(h, ns, monoVertex)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the resourceHealth for MonoVertex %q: %s", monoVertex, err.Error()))
		return
	}

	// Create a new daemon client to get the data status
	client, err := h.getMonoVertexDaemonClient(ns, monoVertex)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for mono vertex %q, %s", monoVertex, err.Error()))
		return
	}
	// Data level health status
	dataHealth, err := client.GetMonoVertexStatus(c)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the mono vertex dataStatus: namespace %q mono vertex %q: %s", ns, monoVertex, err.Error()))
		return
	}

	// Create a response string based on the vertex health and data criticality
	// We combine both the states to get the final dataStatus of the pipeline
	response := NewHealthResponse(resourceHealth.Status, dataHealth.GetStatus(),
		resourceHealth.Message, dataHealth.GetMessage(), resourceHealth.Code, dataHealth.GetCode())

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, response))
}

// GetVertexErrors is used to provide the errors of a given vertex
func (h *handler) GetVertexErrors(c *gin.Context) {
	ns, pipeline, vertex := c.Param("namespace"), c.Param("pipeline"), c.Param("vertex")

	client, err := h.getPipelineDaemonClient(ns, pipeline)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for pipeline %q, %s", pipeline, err.Error()))
		return
	}

	errors, err := client.GetVertexErrors(c, pipeline, vertex)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the errors for pipeline %q vertex %q: %s", pipeline, vertex, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, errors))
}

// GetMonoVertexErrors is used to provide the errors of a given mono vertex
func (h *handler) GetMonoVertexErrors(c *gin.Context) {
	ns, monoVertex := c.Param("namespace"), c.Param("mono-vertex")

	client, err := h.getMonoVertexDaemonClient(ns, monoVertex)
	if err != nil || client == nil {
		h.respondWithError(c, fmt.Sprintf("failed to get daemon service client for mono vertex %q, %s", monoVertex, err.Error()))
		return
	}

	errors, err := client.GetMonoVertexErrors(c, monoVertex)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to get the errors for mono vertex %q: %s", monoVertex, err.Error()))
		return
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, errors))
}

func (h *handler) GetMetricData(c *gin.Context) {
	var requestBody MetricsRequestBody
	if h.promQlServiceObj == nil {
		h.respondWithError(c, "Failed to get the prometheus query service")
		return
	}
	if err := bindJson(c, &requestBody); err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to decode JSON request body to metrics query spec, %s", err.Error()))
		return
	}
	// builds prom query
	promQl, err := h.promQlServiceObj.BuildQuery(requestBody)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to build the prometheus query%v", err))
		return
	}
	// default start time is 30 minutes before the current time
	if requestBody.StartTime == "" {
		requestBody.StartTime = time.Now().Add(-30 * time.Minute).Format(time.RFC3339)
	}
	// default end time is the current time
	if requestBody.EndTime == "" {
		requestBody.EndTime = time.Now().Format(time.RFC3339)
	}

	startTime, _ := time.Parse(time.RFC3339, requestBody.StartTime)
	endTime, _ := time.Parse(time.RFC3339, requestBody.EndTime)

	result, err := h.promQlServiceObj.QueryPrometheus(context.Background(), promQl, startTime, endTime)
	if err != nil {
		h.respondWithError(c, fmt.Sprintf("Failed to execute the prometheus query, %s", err.Error()))
		return
	}
	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, result))
}

// DiscoverMetrics is used to provide a metrics list for each
// dimension along with necessary params and filters for a given object
func (h *handler) DiscoverMetrics(c *gin.Context) {
	// Get the object for which the metrics are to be discovered
	// Ex. mono-vertex, pipeline, etc.
	object := c.Param("object")

	configData := h.promQlServiceObj.GetConfigData()
	if configData == nil {
		h.respondWithError(c, "PrometheusClient metric config is not available")
		return
	}

	var discoveredMetrics MetricsDiscoveryResponse

	for _, pattern := range configData.Patterns {
		if slices.Contains(pattern.Objects, object) {
			for _, metric := range pattern.Metrics {
				var requiredFilters []Filter
				// Populate the required filters
				// TODO (ajain): place a check at filters instead of dimension level
				// so that more patterns can be clubbed
				// OR
				// shift to metrics approach (already tested) instead of patterns approach
				for _, filter := range metric.Filters {
					requiredFilters = append(requiredFilters, Filter{
						Name:     filter,
						Required: true,
					})
				}
				// Computing dimension data for each metric
				var dimensionData []Dimensions
				for _, dimension := range metric.Dimensions {
					// Check if the object is "mono-vertex", skip the "vertex"(pipeline) dimension
					if object == "mono-vertex" && dimension.Name == "vertex" {
						continue
					}
					// Check if the object is "vertex"(pipeline), skip the "mono-vertex" dimension
					if object == "vertex" && dimension.Name == "mono-vertex" {
						continue
					}
					var combinedFilters = requiredFilters
					// Add the dimension filters
					for _, filter := range dimension.Filters {
						combinedFilters = append(combinedFilters, Filter{
							Name:     filter.Name,
							Required: filter.Required,
						})
					}
					dimensionData = append(dimensionData, Dimensions{
						Name:    dimension.Name,
						Filters: combinedFilters,
						Params:  pattern.Params,
					})
				}

				discoveredMetrics = append(discoveredMetrics, NewDiscoveryResponse(pattern.Name, metric.Name, metric.MetricDescription, metric.DisplayName, metric.Unit, dimensionData))
			}
		}
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, discoveredMetrics))
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

// getMonoVertices is a utility used to fetch all the mono vertices in a given namespace
func getMonoVertices(h *handler, namespace string) (MonoVertices, error) {
	mvtList, err := h.numaflowClient.MonoVertices(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var resList MonoVertices
	for _, mvt := range mvtList.Items {
		status, err := getMonoVertexStatus(&mvt)
		if err != nil {
			return nil, err
		}
		resp := NewMonoVertexInfo(status, &mvt)
		resList = append(resList, resp)
	}
	return resList, nil
}

// GetPipelineStatus is used to provide the status of a given pipeline
// TODO(API): Change the Daemon service to return the consolidated status of the pipeline
// to save on multiple calls to the daemon service
func getPipelineStatus(pipeline *dfv1.Pipeline) (string, error) {
	retStatus := dfv1.PipelineStatusHealthy
	// Check if the pipeline is paused, if so, return inactive status
	if pipeline.GetDesiredPhase() == dfv1.PipelinePhasePaused {
		retStatus = dfv1.PipelineStatusInactive
	} else if pipeline.GetDesiredPhase() == dfv1.PipelinePhaseRunning {
		retStatus = dfv1.PipelineStatusHealthy
	} else if pipeline.GetDesiredPhase() == dfv1.PipelinePhaseFailed {
		retStatus = dfv1.PipelineStatusCritical
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

func getMonoVertexStatus(mvt *dfv1.MonoVertex) (string, error) {
	// TODO - add more logic to determine the status of a mono vertex
	return dfv1.MonoVertexStatusHealthy, nil
}

// validatePipelineSpec is used to validate the pipeline spec during create and update
func validatePipelineSpec(h *handler, oldPipeline *dfv1.Pipeline, newPipeline *dfv1.Pipeline, validType string) error {
	ns := newPipeline.Namespace
	isbClient := h.numaflowClient.InterStepBufferServices(ns)
	valid := validator.NewPipelineValidator(isbClient, oldPipeline, newPipeline)
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

func pipelineDaemonSvcAddress(ns, pipelineName string) string {
	// the format is consistent with what we defined in GetDaemonServiceURL in `pkg/apis/numaflow/v1alpha1/pipeline_types.go`
	// do not change it without changing the other.
	return fmt.Sprintf("%s.%s.svc:%d", fmt.Sprintf("%s-daemon-svc", pipelineName), ns, dfv1.DaemonServicePort)
}

func monoVertexDaemonSvcAddress(ns, monoVertexName string) string {
	// the format is consistent with what we defined in GetDaemonServiceURL in `pkg/apis/numaflow/v1alpha1/mono_vertex_types.go`
	// do not change it without changing the other.
	return fmt.Sprintf("%s.%s.svc:%d", fmt.Sprintf("%s-mv-daemon-svc", monoVertexName), ns, dfv1.MonoVertexDaemonServicePort)
}

func (h *handler) getPipelineDaemonClient(ns, pipeline string) (daemonclient.DaemonClient, error) {
	if dClient, ok := h.daemonClientsCache.Get(pipelineDaemonSvcAddress(ns, pipeline)); !ok {
		var err error
		var c daemonclient.DaemonClient
		// Default to use gRPC client
		if strings.EqualFold(h.opts.daemonClientProtocol, "http") {
			c, err = daemonclient.NewRESTfulDaemonServiceClient(pipelineDaemonSvcAddress(ns, pipeline))
		} else {
			c, err = daemonclient.NewGRPCDaemonServiceClient(pipelineDaemonSvcAddress(ns, pipeline))
		}
		if err != nil {
			return nil, err
		}
		h.daemonClientsCache.Add(pipelineDaemonSvcAddress(ns, pipeline), c)
		return c, nil
	} else {
		return dClient, nil
	}
}

func (h *handler) getMonoVertexDaemonClient(ns, mvtName string) (mvtdaemonclient.MonoVertexDaemonClient, error) {
	if mvtDaemonClient, ok := h.mvtDaemonClientsCache.Get(monoVertexDaemonSvcAddress(ns, mvtName)); !ok {
		var err error
		var c mvtdaemonclient.MonoVertexDaemonClient
		// Default to use gRPC client
		if strings.EqualFold(h.opts.daemonClientProtocol, "http") {
			c, err = mvtdaemonclient.NewRESTfulClient(monoVertexDaemonSvcAddress(ns, mvtName))
		} else {
			c, err = mvtdaemonclient.NewGRPCClient(monoVertexDaemonSvcAddress(ns, mvtName))
		}
		if err != nil {
			return nil, err
		}
		h.mvtDaemonClientsCache.Add(monoVertexDaemonSvcAddress(ns, mvtName), c)
		return c, nil
	} else {
		return mvtDaemonClient, nil
	}
}

func (h *handler) getPodDetails(pod corev1.Pod) (PodDetails, error) {
	podDetails := PodDetails{
		Name:    pod.Name,
		Status:  string(pod.Status.Phase),
		Message: pod.Status.Message,
		Reason:  pod.Status.Reason,
	}

	metricsClient := h.metricsClient

	// container details of a pod
	containerDetails := h.getContainerDetails(pod)
	podDetails.ContainerDetailsMap = containerDetails

	// cpu/memory details of a pod
	podMetrics, err := metricsClient.PodMetricses(pod.Namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	if err == nil {
		totalCPU := resource.NewQuantity(0, resource.DecimalSI)
		totalMemory := resource.NewQuantity(0, resource.BinarySI)
		for _, container := range podMetrics.Containers {
			containerName := container.Name
			cpuQuantity := container.Usage.Cpu()
			memQuantity := container.Usage.Memory()
			details, ok := containerDetails[containerName]
			if !ok {
				details = ContainerDetails{Name: container.Name} // Initialize if not found
			}
			if cpuQuantity != nil {
				details.TotalCPU = strconv.FormatInt(cpuQuantity.MilliValue(), 10) + "m"
				totalCPU.Add(*cpuQuantity)
			}
			if memQuantity != nil {
				details.TotalMemory = strconv.FormatInt(memQuantity.Value()/(1024*1024), 10) + "Mi"
				totalMemory.Add(*memQuantity)
			}
			containerDetails[containerName] = details
		}
		if totalCPU != nil {
			podDetails.TotalCPU = strconv.FormatInt(totalCPU.MilliValue(), 10) + "m"
		}

		if totalMemory != nil {
			podDetails.TotalMemory = strconv.FormatInt(totalMemory.Value()/(1024*1024), 10) + "Mi"
		}
	}
	return podDetails, nil
}

func (h *handler) getContainerDetails(pod corev1.Pod) map[string]ContainerDetails {
	totalContainers := len(pod.Spec.Containers) + len(pod.Spec.InitContainers)
	containerDetailsMap := make(map[string]ContainerDetails, totalContainers)

	// Helper function to process container statuses
	processContainerStatus := func(status corev1.ContainerStatus, isInitContainer bool) {
		// Skip init containers without Always restart policy
		if isInitContainer && h.isNotSidecarContainer(status.Name, pod) {
			return
		}

		containerName := status.Name
		details := ContainerDetails{
			Name:         status.Name,
			ID:           status.ContainerID,
			State:        h.getContainerStatus(status.State),
			RestartCount: status.RestartCount,
		}
		// Reason the container is not yet running
		// Message regarding why the container is not yet running
		if status.State.Waiting != nil {
			details.WaitingReason = status.State.Waiting.Reason
			details.WaitingMessage = status.State.Waiting.Message
		}
		// Details about a terminated container
		// Reason from the last termination
		// Message regarding the last termination
		// Exit status from last termination
		if status.LastTerminationState.Terminated != nil {
			details.LastTerminationReason = status.LastTerminationState.Terminated.Reason
			details.LastTerminationMessage = status.LastTerminationState.Terminated.Message
			details.LastTerminationExitCode = &status.LastTerminationState.Terminated.ExitCode
		}
		// Time at which the container last (re-)started
		if status.State.Running != nil {
			details.LastStartedAt = status.State.Running.StartedAt.Format(time.RFC3339)
		}
		containerDetailsMap[containerName] = details
	}

	for _, status := range pod.Status.InitContainerStatuses {
		processContainerStatus(status, true)
	}
	for _, status := range pod.Status.ContainerStatuses {
		processContainerStatus(status, false)
	}

	// Helper function to process container resources
	processContainerResources := func(container corev1.Container, isInitContainer bool) {
		// Skip init containers without Always restart policy
		if isInitContainer && h.isNotSidecarContainer(container.Name, pod) {
			return
		}

		cpuRequest := container.Resources.Requests.Cpu().MilliValue()
		memRequest := container.Resources.Requests.Memory().Value() / (1024 * 1024)
		cpuLimit := container.Resources.Limits.Cpu().MilliValue()
		memLimit := container.Resources.Limits.Memory().Value() / (1024 * 1024)

		details, ok := containerDetailsMap[container.Name]
		if !ok {
			details = ContainerDetails{Name: container.Name} // Initialize if not found
		}
		if cpuRequest != 0 {
			details.RequestedCPU = strconv.FormatInt(cpuRequest, 10) + "m"
		}
		if memRequest != 0 {
			details.RequestedMemory = strconv.FormatInt(memRequest, 10) + "Mi"
		}
		if cpuLimit != 0 {
			details.LimitCPU = strconv.FormatInt(cpuLimit, 10) + "m"
		}
		if memLimit != 0 {
			details.LimitMemory = strconv.FormatInt(memLimit, 10) + "Mi"
		}
		containerDetailsMap[container.Name] = details
	}

	for _, container := range pod.Spec.InitContainers {
		processContainerResources(container, true)
	}
	for _, container := range pod.Spec.Containers {
		processContainerResources(container, false)
	}

	return containerDetailsMap
}

// Helper function to check if container is an init container without "Always" restart policy
// for sidecar containers (eg: ud containers), we set "Always" restart policy in k8s >= 1.29
func (h *handler) isNotSidecarContainer(containerName string, pod corev1.Pod) bool {
	for _, initContainer := range pod.Spec.InitContainers {
		if initContainer.Name == containerName {
			return initContainer.RestartPolicy == nil || *initContainer.RestartPolicy != corev1.ContainerRestartPolicyAlways
		}
	}
	return true
}

func (h *handler) getContainerStatus(state corev1.ContainerState) string {
	if state.Running != nil {
		return "Running"
	} else if state.Waiting != nil {
		return "Waiting"
	} else if state.Terminated != nil {
		return "Terminated"
	} else {
		return "Unknown"
	}
}
