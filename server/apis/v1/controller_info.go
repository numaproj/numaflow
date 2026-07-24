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
//  2. If missing, list with label app.kubernetes.io/component=controller-manager.
//  3. If that yields nothing, list all Deployments and match template/selector labels
//     (standard manifests put component labels on the pod template, not always on metadata).
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
		dep, err = h.findControllerDeploymentInNamespace(ctx, ns)
		if err != nil {
			h.respondWithError(c, fmt.Sprintf("Failed to list controller deployments in namespace %q, %s", ns, err.Error()))
			return
		}
		if dep == nil {
			c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, NewMissingControllerInfo(ns)))
			return
		}
	}

	c.JSON(http.StatusOK, NewNumaflowAPIResponse(nil, extractControllerInfo(ns, dep, resolveEnvLookup(ctx, h.kubeClient, ns))))
}

// findControllerDeploymentInNamespace locates a renamed controller Deployment.
// Prefer a server-side label selector; fall back to scanning template/selector labels
// when Deployments lack metadata labels (standard Numaflow install manifests).
func (h *handler) findControllerDeploymentInNamespace(ctx context.Context, ns string) (*appsv1.Deployment, error) {
	selector := fmt.Sprintf("%s=%s", dfv1.KeyComponent, dfv1.ComponentControllerManager)
	list, err := h.kubeClient.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	if dep := findControllerDeployment(list.Items); dep != nil {
		return dep, nil
	}

	list, err = h.kubeClient.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return findControllerDeployment(list.Items), nil
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
