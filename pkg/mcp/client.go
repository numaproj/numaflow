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

// Package mcp implements a read-only Model Context Protocol (MCP) server for
// Numaflow. It exposes read-only tools (CRD list/get, daemon-backed runtime
// diagnostics, and Kubernetes pod/log/event tools) so that MCP clients
// (e.g. Cursor, Claude Code) can inspect Numaflow resources without log scraping.
//
// The server performs no create, update, delete or patch operations.
package mcp

import (
	"fmt"

	"k8s.io/client-go/kubernetes"

	dfv1versiond "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// NewClients builds a Kubernetes core client and a Numaflow typed client from
// the ambient kubeconfig (KUBECONFIG or ~/.kube/config) or, when running
// inside a cluster, the in-cluster config.
func NewClients() (kubernetes.Interface, dfv1clients.NumaflowV1alpha1Interface, error) {
	restConfig, err := util.K8sRestConfig()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get kubernetes rest config: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}
	clientset, err := dfv1versiond.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create numaflow client: %w", err)
	}
	return kubeClient, clientset.NumaflowV1alpha1(), nil
}
