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

package poddiscovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/numaproj/numaflow/pkg/shared/logging"
)

var discoveryLog = logging.NewLogger().Named("PodDiscovery")

const defaultLookupTimeout = 2 * time.Second

// ResolveRequest identifies the headless service to discover replicas from.
type ResolveRequest struct {
	HeadlessService string
	Namespace       string
}

// MonoVertexRequest returns the DNS discovery request for a MonoVertex.
func MonoVertexRequest(name, namespace string) ResolveRequest {
	return ResolveRequest{
		HeadlessService: fmt.Sprintf("%s-mv-headless", name),
		Namespace:       namespace,
	}
}

// PipelineVertexRequest returns the DNS discovery request for a Pipeline vertex.
func PipelineVertexRequest(pipelineName, vertexName, namespace string) ResolveRequest {
	return ResolveRequest{
		HeadlessService: fmt.Sprintf("%s-%s-headless", pipelineName, vertexName),
		Namespace:       namespace,
	}
}

// Resolver discovers replica indices from a headless Service's DNS records.
// Replica indices are assumed to be contiguous from 0 to len(ips)-1.
type Resolver interface {
	Resolve(ctx context.Context, req ResolveRequest) ([]int, error)
}

type hostLookup interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

type dnsResolver struct {
	lookup  hostLookup
	timeout time.Duration
}

// NewResolver returns a Resolver backed by the pod's configured DNS resolver.
func NewResolver() Resolver {
	return &dnsResolver{
		lookup:  net.DefaultResolver,
		timeout: defaultLookupTimeout,
	}
}

func (r *dnsResolver) Resolve(ctx context.Context, req ResolveRequest) ([]int, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	host := fmt.Sprintf("%s.%s.svc", req.HeadlessService, req.Namespace)
	discoveryLog.Debugf("Resolving replicas for service=%s", host)
	ips, err := r.lookup.LookupHost(lookupCtx, host)
	if err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			return []int{}, nil
		}
		return nil, fmt.Errorf("failed to resolve host %s: %w", host, err)
	}

	indices := make([]int, len(ips))
	for i := range indices {
		indices[i] = i
	}
	discoveryLog.Debugf("DNS resolve returned %d replicas for service=%s", len(indices), host)
	return indices, nil
}
