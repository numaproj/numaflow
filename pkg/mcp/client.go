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
// Numaflow. It exposes a small set of read-only tools (list/get Pipelines,
// MonoVertices and InterStepBufferServices) backed by the Kubernetes API so
// that MCP clients (e.g. Cursor, Claude) can inspect Numaflow resources.
//
// The server performs no create, update, delete or patch operations: it is
// read-only by construction.
package mcp

import (
	"fmt"

	dfv1versiond "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/shared/util"
)

// NewNumaflowClient builds a Numaflow typed client from the ambient kubeconfig
// (KUBECONFIG or ~/.kube/config) or, when running inside a cluster, the
// in-cluster config.
func NewNumaflowClient() (dfv1clients.NumaflowV1alpha1Interface, error) {
	restConfig, err := util.K8sRestConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubernetes rest config: %w", err)
	}
	clientset, err := dfv1versiond.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create numaflow client: %w", err)
	}
	return clientset.NumaflowV1alpha1(), nil
}
