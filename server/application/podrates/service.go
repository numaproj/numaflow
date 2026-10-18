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

package podrates

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	"github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	"github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
	dfv1clients "github.com/numaproj/numaflow/pkg/client/clientset/versioned/typed/numaflow/v1alpha1"
)

// ErrDaemonUnavailable means the vertex exists but its daemon could not be reached.
// Callers map it to a retryable 503 and do not echo the underlying error.
var ErrDaemonUnavailable = errors.New("vertex daemon is unavailable")

// Service loads a Vertex or MonoVertex, then reads per-pod rates from its daemon.
type Service struct {
	numaflow dfv1clients.NumaflowV1alpha1Interface
	daemons  DaemonClients
	now      func() time.Time
}

// NewService constructs the pod read-rate service.
func NewService(numaflow dfv1clients.NumaflowV1alpha1Interface, daemons DaemonClients) (*Service, error) {
	if numaflow == nil {
		return nil, fmt.Errorf("numaflow client is required")
	}
	if daemons == nil {
		return nil, fmt.Errorf("daemon clients are required")
	}
	return &Service{numaflow: numaflow, daemons: daemons, now: time.Now}, nil
}

// GetMonoVertexPodReadRates returns per-pod 5m and 15m read rates for a MonoVertex.
func (s *Service) GetMonoVertexPodReadRates(ctx context.Context, namespace, monoVertex string) (PodReadRates, error) {
	if _, err := s.numaflow.MonoVertices(namespace).Get(ctx, monoVertex, metav1.GetOptions{}); err != nil {
		return PodReadRates{}, err
	}
	client, err := s.daemons.MonoVertexDaemon(namespace, monoVertex)
	if err != nil {
		return PodReadRates{}, fmt.Errorf("%w: %w", ErrDaemonUnavailable, err)
	}
	pods, err := client.GetMonoVertexPodReadRates(ctx)
	if err != nil {
		return PodReadRates{}, fmt.Errorf("%w: %w", ErrDaemonUnavailable, err)
	}
	return PodReadRates{ObservedAt: s.now(), Pods: fromMonoVertexPods(pods)}, nil
}

// GetPipelineVertexPodReadRates returns per-pod 5m and 15m read rates for one pipeline vertex.
// The vertex argument is the logical name; the custom resource name is "{pipeline}-{vertex}".
func (s *Service) GetPipelineVertexPodReadRates(ctx context.Context, namespace, pipeline, vertex string) (PodReadRates, error) {
	if _, err := s.getPipelineVertex(ctx, namespace, pipeline, vertex); err != nil {
		return PodReadRates{}, err
	}
	client, err := s.daemons.PipelineDaemon(namespace, pipeline)
	if err != nil {
		return PodReadRates{}, fmt.Errorf("%w: %w", ErrDaemonUnavailable, err)
	}
	pods, err := client.GetVertexPodReadRates(ctx, pipeline, vertex)
	if err != nil {
		return PodReadRates{}, fmt.Errorf("%w: %w", ErrDaemonUnavailable, err)
	}
	return PodReadRates{ObservedAt: s.now(), Pods: fromPipelinePods(pods)}, nil
}

func (s *Service) getPipelineVertex(ctx context.Context, namespace, pipeline, vertex string) (*dfv1.Vertex, error) {
	resourceName := fmt.Sprintf("%s-%s", pipeline, vertex)
	resource, err := s.numaflow.Vertices(namespace).Get(ctx, resourceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if resource.Spec.PipelineName != pipeline || resource.Spec.Name != vertex {
		return nil, apierrors.NewNotFound(dfv1.Resource("vertices"), resourceName)
	}
	return resource, nil
}

func fromMonoVertexPods(pods []*mvtxdaemon.PodReadRate) []PodReadRate {
	rates := make([]PodReadRate, 0, len(pods))
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		rates = append(rates, PodReadRate{
			Replica:  int(pod.Replica),
			Hostname: pod.Hostname,
			Rate5m:   windowRate(pod.ReadRates, "5m"),
			Rate15m:  windowRate(pod.ReadRates, "15m"),
		})
	}
	return rates
}

func fromPipelinePods(pods []*daemon.PodReadRate) []PodReadRate {
	rates := make([]PodReadRate, 0, len(pods))
	for _, pod := range pods {
		if pod == nil {
			continue
		}
		rates = append(rates, PodReadRate{
			Replica:  int(pod.Replica),
			Hostname: pod.Hostname,
			Rate5m:   windowRate(pod.ReadRates, "5m"),
			Rate15m:  windowRate(pod.ReadRates, "15m"),
		})
	}
	return rates
}

func windowRate(windows map[string]*wrapperspb.DoubleValue, window string) *float64 {
	value := windows[window]
	if value == nil {
		return nil
	}
	rate := value.GetValue()
	return &rate
}
