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
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

// GetControllerInfo returns version and scope for the numaflow-controller Deployment in a namespace.
//
// Discovery policy:
//  1. Get Deployment by the canonical name "numaflow-controller" (standard install manifests).
//  2. If missing, list by label app.kubernetes.io/component=controller-manager (renamed installs).
//
// A missing controller returns HTTP 200 with found:false so the UI can retry another namespace
// without treating "not installed here" as an API failure.
func (h *handler) GetControllerInfo(c *gin.Context) {
	ns := c.Param("namespace")
	ctx := c.Request.Context()

	dep, err := h.kubeClient.AppsV1().Deployments(ns).Get(ctx, controllerDeploymentName, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			h.respondWithError(c, fmt.Sprintf("Failed to get controller deployment in namespace %q, %s", ns, err.Error()))
			return
		}
		// Name miss: install manifests put component labels on the pod template, not always on
		// Deployment.metadata, so list the namespace and match template/selector labels.
		list, listErr := h.kubeClient.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
		if listErr != nil {
			h.respondWithError(c, fmt.Sprintf("Failed to list controller deployments in namespace %q, %s", ns, listErr.Error()))
			return
		}
		dep = findControllerDeployment(list.Items)
		if dep == nil {
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, NewMissingControllerInfo(ns)))
			return
		}
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, extractControllerInfo(ns, dep)))
}

// findControllerDeployment picks a Deployment whose pod template or selector identifies
// the Numaflow controller-manager (covers renamed Deployments that keep standard labels).
func findControllerDeployment(items []appsv1.Deployment) *appsv1.Deployment {
	for i := range items {
		dep := &items[i]
		if dep.Labels[dfv1.KeyComponent] == dfv1.ComponentControllerManager {
			return dep
		}
		if dep.Spec.Selector != nil && dep.Spec.Selector.MatchLabels[dfv1.KeyComponent] == dfv1.ComponentControllerManager {
			return dep
		}
		if dep.Spec.Template.Labels[dfv1.KeyComponent] == dfv1.ComponentControllerManager {
			return dep
		}
	}
	return nil
}
