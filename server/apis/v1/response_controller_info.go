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
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
)

const (
	// controllerDeploymentName is the canonical Deployment name from Numaflow install manifests.
	controllerDeploymentName = "numaflow-controller"
	// controllerContainerName is the container that runs the "controller" subcommand.
	controllerContainerName = "controller-manager"
	envControllerNamespaced      = "NUMAFLOW_CONTROLLER_NAMESPACED"
	envControllerManagedNamespace = "NUMAFLOW_CONTROLLER_MANAGED_NAMESPACE"
)

// ControllerInfo is the response payload for GET .../controller-info.
// It describes the live numaflow-controller Deployment in a namespace (not the UX server).
type ControllerInfo struct {
	Found            bool   `json:"found"`
	Namespace        string `json:"namespace"`
	Name             string `json:"name,omitempty"`
	Version          string `json:"version,omitempty"`
	Image            string `json:"image,omitempty"`
	Namespaced       bool   `json:"namespaced"`
	ManagedNamespace string `json:"managedNamespace,omitempty"`
	Replicas         *int32 `json:"replicas,omitempty"`
}

// NewMissingControllerInfo returns a successful "not found" payload.
// found:false is intentional success so the UI can fall back to another namespace
// (e.g. numaflow-system for cluster-scoped installs) without treating it as an API error.
func NewMissingControllerInfo(namespace string) ControllerInfo {
	return ControllerInfo{
		Found:     false,
		Namespace: namespace,
	}
}

// extractControllerInfo builds ControllerInfo from a discovered controller Deployment.
func extractControllerInfo(namespace string, dep *appsv1.Deployment) ControllerInfo {
	info := ControllerInfo{
		Found:     true,
		Namespace: namespace,
		Name:      dep.Name,
		Replicas:  dep.Spec.Replicas,
	}

	container := findControllerContainer(dep.Spec.Template.Spec.Containers)
	if container == nil && len(dep.Spec.Template.Spec.Containers) > 0 {
		container = &dep.Spec.Template.Spec.Containers[0]
	}
	if container == nil {
		return info
	}

	info.Image = container.Image
	info.Version = parseImageVersion(container.Image)
	// Prefer NUMAFLOW_IMAGE when the image ref has no usable tag (e.g. digest-only).
	if info.Version == "" {
		if img := envValue(container.Env, dfv1.EnvImage); img != "" {
			info.Image = img
			info.Version = parseImageVersion(img)
		}
	}

	namespaced, managedNS := controllerScopeFromPodSpec(dep.Spec.Template.Spec)
	info.Namespaced = namespaced
	if managedNS != "" {
		info.ManagedNamespace = managedNS
	} else if namespaced {
		// When namespaced with no explicit managed-namespace, the controller watches its own ns.
		info.ManagedNamespace = namespace
	}
	return info
}

// findControllerContainer prefers the well-known controller-manager container name.
func findControllerContainer(containers []corev1.Container) *corev1.Container {
	for i := range containers {
		if containers[i].Name == controllerContainerName {
			return &containers[i]
		}
	}
	return nil
}

// parseImageVersion extracts the tag from an image reference.
// Digest-only refs (image@sha256:...) have no tag, so we return empty rather than a misleading digest fragment.
func parseImageVersion(image string) string {
	if image == "" {
		return ""
	}
	// Strip digest first so "repo:tag@sha256:..." still yields the tag.
	if at := strings.LastIndex(image, "@"); at >= 0 {
		image = image[:at]
	}
	// Registry hosts may contain ":", so only treat the segment after the last "/" as having a tag.
	slash := strings.LastIndex(image, "/")
	name := image
	if slash >= 0 {
		name = image[slash+1:]
	}
	colon := strings.LastIndex(name, ":")
	if colon < 0 {
		return ""
	}
	return name[colon+1:]
}

// controllerScopeFromPodSpec derives namespaced mode and managed namespace.
// Args override env because the controller binary gives CLI flags precedence over ConfigMap-backed env.
func controllerScopeFromPodSpec(spec corev1.PodSpec) (namespaced bool, managedNamespace string) {
	var container *corev1.Container
	if c := findControllerContainer(spec.Containers); c != nil {
		container = c
	} else if len(spec.Containers) > 0 {
		container = &spec.Containers[0]
	}
	if container == nil {
		return false, ""
	}

	namespacedFromArgs, managedFromArgs, argsSet := scopeFromArgs(container.Args)
	if argsSet {
		return namespacedFromArgs, managedFromArgs
	}

	if v := envValue(container.Env, envControllerNamespaced); v != "" {
		namespaced, _ = strconv.ParseBool(v)
	}
	managedNamespace = envValue(container.Env, envControllerManagedNamespace)
	return namespaced, managedNamespace
}

// scopeFromArgs parses --namespaced / --managed-namespace from container args.
// argsSet is true when either flag appears so callers know args should win over env.
func scopeFromArgs(args []string) (namespaced bool, managedNamespace string, argsSet bool) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--namespaced":
			namespaced = true
			argsSet = true
		case strings.HasPrefix(arg, "--namespaced="):
			namespaced, _ = strconv.ParseBool(strings.TrimPrefix(arg, "--namespaced="))
			argsSet = true
		case arg == "--managed-namespace" && i+1 < len(args):
			managedNamespace = args[i+1]
			argsSet = true
			i++
		case strings.HasPrefix(arg, "--managed-namespace="):
			managedNamespace = strings.TrimPrefix(arg, "--managed-namespace=")
			argsSet = true
		}
	}
	return namespaced, managedNamespace, argsSet
}

func envValue(env []corev1.EnvVar, name string) string {
	for _, e := range env {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
