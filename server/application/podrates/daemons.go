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
	"fmt"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtxclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
)

const daemonClientCacheSize = 500

// DaemonClients opens cached connections to pipeline and MonoVertex daemons.
// Concurrent cache misses for one address share a single dial. A client that loses a
// replacement is closed, because the LRU does not close a value overwritten by Add.
type DaemonClients interface {
	MonoVertexDaemon(namespace, name string) (mvtxclient.MonoVertexDaemonClient, error)
	PipelineDaemon(namespace, name string) (daemonclient.DaemonClient, error)
}

type daemonClients struct {
	protocol       string
	mono           *lru.Cache[string, mvtxclient.MonoVertexDaemonClient]
	pipeline       *lru.Cache[string, daemonclient.DaemonClient]
	monoFlight     singleflight.Group
	pipelineFlight singleflight.Group
}

// Constructors are variables so tests can count clients created on a cache miss.
var newMonoVertexDaemonClient = func(protocol, address string) (mvtxclient.MonoVertexDaemonClient, error) {
	if strings.EqualFold(protocol, "http") {
		return mvtxclient.NewRESTfulClient(address)
	}
	return mvtxclient.NewGRPCClient(address)
}

var newPipelineDaemonClient = func(protocol, address string) (daemonclient.DaemonClient, error) {
	if strings.EqualFold(protocol, "http") {
		return daemonclient.NewRESTfulDaemonServiceClient(address)
	}
	return daemonclient.NewGRPCDaemonServiceClient(address)
}

// NewDaemonClients caches daemon clients by service address. protocol is "grpc" or "http",
// matching the server's existing daemon client setting.
func NewDaemonClients(protocol string) (DaemonClients, error) {
	mono, err := lru.NewWithEvict(daemonClientCacheSize, func(_ string, client mvtxclient.MonoVertexDaemonClient) {
		_ = client.Close()
	})
	if err != nil {
		return nil, fmt.Errorf("create MonoVertex daemon client cache: %w", err)
	}
	pipeline, err := lru.NewWithEvict(daemonClientCacheSize, func(_ string, client daemonclient.DaemonClient) {
		_ = client.Close()
	})
	if err != nil {
		return nil, fmt.Errorf("create pipeline daemon client cache: %w", err)
	}
	return &daemonClients{protocol: protocol, mono: mono, pipeline: pipeline}, nil
}

func (c *daemonClients) MonoVertexDaemon(namespace, name string) (mvtxclient.MonoVertexDaemonClient, error) {
	address := dfv1.MonoVertex{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}.GetDaemonServiceURL()
	if client, ok := c.mono.Get(address); ok {
		return client, nil
	}
	created, err, _ := c.monoFlight.Do(address, func() (any, error) {
		if client, ok := c.mono.Get(address); ok {
			return client, nil
		}
		client, err := newMonoVertexDaemonClient(c.protocol, address)
		if err != nil {
			return nil, err
		}
		// Add replaces an existing key without running the eviction callback, so close the loser.
		if existing, ok := c.mono.Get(address); ok {
			_ = client.Close()
			return existing, nil
		}
		c.mono.Add(address, client)
		return client, nil
	})
	if err != nil {
		return nil, err
	}
	return created.(mvtxclient.MonoVertexDaemonClient), nil
}

func (c *daemonClients) PipelineDaemon(namespace, name string) (daemonclient.DaemonClient, error) {
	address := dfv1.Pipeline{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}.GetDaemonServiceURL()
	if client, ok := c.pipeline.Get(address); ok {
		return client, nil
	}
	created, err, _ := c.pipelineFlight.Do(address, func() (any, error) {
		if client, ok := c.pipeline.Get(address); ok {
			return client, nil
		}
		client, err := newPipelineDaemonClient(c.protocol, address)
		if err != nil {
			return nil, err
		}
		if existing, ok := c.pipeline.Get(address); ok {
			_ = client.Close()
			return existing, nil
		}
		c.pipeline.Add(address, client)
		return client, nil
	})
	if err != nil {
		return nil, err
	}
	return created.(daemonclient.DaemonClient), nil
}
