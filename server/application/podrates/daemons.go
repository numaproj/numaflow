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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtxclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
)

const daemonClientCacheSize = 500

// DaemonClients opens cached connections to pipeline and MonoVertex daemons.
type DaemonClients interface {
	MonoVertexDaemon(namespace, name string) (mvtxclient.MonoVertexDaemonClient, error)
	PipelineDaemon(namespace, name string) (daemonclient.DaemonClient, error)
}

type daemonClients struct {
	protocol string
	mono     *lru.Cache[string, mvtxclient.MonoVertexDaemonClient]
	pipeline *lru.Cache[string, daemonclient.DaemonClient]
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
	client, err := newMonoVertexDaemonClient(c.protocol, address)
	if err != nil {
		return nil, err
	}
	c.mono.Add(address, client)
	return client, nil
}

func (c *daemonClients) PipelineDaemon(namespace, name string) (daemonclient.DaemonClient, error) {
	address := dfv1.Pipeline{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}.GetDaemonServiceURL()
	if client, ok := c.pipeline.Get(address); ok {
		return client, nil
	}
	client, err := newPipelineDaemonClient(c.protocol, address)
	if err != nil {
		return nil, err
	}
	c.pipeline.Add(address, client)
	return client, nil
}

func newMonoVertexDaemonClient(protocol, address string) (mvtxclient.MonoVertexDaemonClient, error) {
	if strings.EqualFold(protocol, "http") {
		return mvtxclient.NewRESTfulClient(address)
	}
	return mvtxclient.NewGRPCClient(address)
}

func newPipelineDaemonClient(protocol, address string) (daemonclient.DaemonClient, error) {
	if strings.EqualFold(protocol, "http") {
		return daemonclient.NewRESTfulDaemonServiceClient(address)
	}
	return daemonclient.NewGRPCDaemonServiceClient(address)
}
