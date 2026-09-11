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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultLookupTimeout = 2 * time.Second

// ResolveRequest identifies the SRV records and indexed pod hostnames to discover.
type ResolveRequest struct {
	HeadlessService string
	Namespace       string
	PortName        string
	PodNamePrefix   string
}

// MonoVertexRequest returns the DNS discovery request for a MonoVertex.
func MonoVertexRequest(name, namespace, portName string) ResolveRequest {
	return ResolveRequest{
		HeadlessService: fmt.Sprintf("%s-mv-headless", name),
		Namespace:       namespace,
		PortName:        portName,
		PodNamePrefix:   fmt.Sprintf("%s-mv-", name),
	}
}

// PipelineVertexRequest returns the DNS discovery request for a Pipeline vertex.
func PipelineVertexRequest(pipelineName, vertexName, namespace, portName string) ResolveRequest {
	prefix := fmt.Sprintf("%s-%s-", pipelineName, vertexName)
	return ResolveRequest{
		HeadlessService: prefix + "headless",
		Namespace:       namespace,
		PortName:        portName,
		PodNamePrefix:   prefix,
	}
}

// Resolver discovers replica indices from a headless Service's SRV records.
type Resolver interface {
	Resolve(ctx context.Context, req ResolveRequest) ([]int, error)
}

type srvLookup interface {
	LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error)
}

type dnsResolver struct {
	lookup  srvLookup
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

	serviceName := fmt.Sprintf("%s.%s.svc", req.HeadlessService, req.Namespace)
	_, records, err := r.lookup.LookupSRV(lookupCtx, req.PortName, "tcp", serviceName)
	if err != nil {
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
			return []int{}, nil
		}
		return nil, fmt.Errorf("failed to resolve SRV records for %s: %w", serviceName, err)
	}

	indices := make(map[int]struct{}, len(records))
	for _, record := range records {
		hostname := strings.SplitN(strings.TrimSuffix(record.Target, "."), ".", 2)[0]
		replica := strings.TrimPrefix(hostname, req.PodNamePrefix)
		if replica == hostname {
			continue
		}
		index, err := strconv.Atoi(replica)
		if err != nil || index < 0 {
			continue
		}
		indices[index] = struct{}{}
	}
	if len(records) > 0 && len(indices) == 0 {
		return nil, fmt.Errorf("SRV records for %s did not contain indexed pods with prefix %q", serviceName, req.PodNamePrefix)
	}

	result := make([]int, 0, len(indices))
	for index := range indices {
		result = append(result, index)
	}
	sort.Ints(result)
	return result, nil
}

// ResolveAndProbe discovers replica candidates and returns indices that pass the probe.
func ResolveAndProbe(ctx context.Context, resolver Resolver, req ResolveRequest, probe func(index int) bool) ([]int, error) {
	candidates, err := resolver.Resolve(ctx, req)
	if err != nil {
		return nil, err
	}
	return probeActive(candidates, probe), nil
}

func probeActive(indices []int, probe func(index int) bool) []int {
	activeByPosition := make([]bool, len(indices))
	var wg sync.WaitGroup
	wg.Add(len(indices))
	for position, index := range indices {
		go func() {
			defer wg.Done()
			activeByPosition[position] = probe(index)
		}()
	}
	wg.Wait()

	active := make([]int, 0, len(indices))
	for position, isActive := range activeByPosition {
		if isActive {
			active = append(active, indices[position])
		}
	}
	sort.Ints(active)
	return active
}
