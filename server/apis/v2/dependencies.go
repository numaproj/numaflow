/*
Copyright 2026 The Numaproj Authors.

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

package v2

import (
	"fmt"

	dfv1versioned "github.com/numaproj/numaflow/pkg/client/clientset/versioned"
	"github.com/numaproj/numaflow/pkg/shared/util"
	"github.com/numaproj/numaflow/server/application/capabilities"
	"github.com/numaproj/numaflow/server/application/observability"
)

// NewClusterHandler assembles the API v2 handler from server capabilities and observability.
func NewClusterHandler() (*Handler, error) {
	restConfig, err := util.K8sRestConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get Kubernetes REST config: %w", err)
	}
	numaflowClient, err := dfv1versioned.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Numaflow client: %w", err)
	}
	summaryService, err := observability.NewService(numaflowClient.NumaflowV1alpha1())
	if err != nil {
		return nil, err
	}
	return NewHandler(capabilities.NewService(), summaryService)
}
