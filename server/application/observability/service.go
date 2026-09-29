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

package observability

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
)

// Service loads Vertex (and later MonoVertex) CRs and maps them to API v2 DTOs.
type Service struct {
	numaflowClient dfv1clients.NumaflowV1alpha1Interface
}

// NewService constructs the observability application service.
func NewService(numaflowClient dfv1clients.NumaflowV1alpha1Interface) (*Service, error) {
	if numaflowClient == nil {
		return nil, fmt.Errorf("numaflow client is required")
	}
	return &Service{numaflowClient: numaflowClient}, nil
}

// getPipelineVertex loads the Vertex CR for a pipeline graph vertex. The path
// uses the logical vertex name; the CR name is "{pipeline}-{vertex}". A second
// spec check avoids false matches when pipeline/vertex names contain hyphens.
func (s *Service) getPipelineVertex(ctx context.Context, namespace, pipeline, vertex string) (*dfv1.Vertex, error) {
	resourceName := fmt.Sprintf("%s-%s", pipeline, vertex)
	resource, err := s.numaflowClient.Vertices(namespace).Get(ctx, resourceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if resource.Spec.PipelineName != pipeline || resource.Spec.Name != vertex {
		return nil, apierrors.NewNotFound(dfv1.Resource("vertices"), resourceName)
	}
	return resource, nil
}
