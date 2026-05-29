/*
Copyright 2022 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the License for the specific language governing permissions and
limitations under the License.
*/

package mcp

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// daemonConnector resolves in-cluster daemon service addresses to a dialable
// endpoint. When the MCP server runs outside the cluster (e.g. stdio mode with
// a local kubeconfig), cluster DNS names like "pipeline-daemon-svc.ns.svc:4327"
// do not resolve; in that case a cached Kubernetes port-forward to the daemon
// pod is established instead.
type daemonConnector struct {
	restConfig *rest.Config
	kubeClient kubernetes.Interface

	mu       sync.Mutex
	forwards map[string]*daemonPortForward
}

type daemonPortForward struct {
	localAddr string
	stopCh    chan struct{}
}

func newDaemonConnector(restConfig *rest.Config, kubeClient kubernetes.Interface) *daemonConnector {
	return &daemonConnector{
		restConfig: restConfig,
		kubeClient: kubeClient,
		forwards:   make(map[string]*daemonPortForward),
	}
}

func isInCluster() bool {
	_, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token")
	return err == nil
}

// resolve returns a dialable address for the daemon service. In-cluster callers
// receive the original cluster DNS address unchanged. Out-of-cluster callers
// receive a localhost port-forward address when one can be established; if
// port-forward setup fails the original address is returned as a fallback.
func (c *daemonConnector) resolve(ctx context.Context, clusterAddr string) (string, error) {
	if c == nil || isInCluster() {
		return clusterAddr, nil
	}
	addr, err := c.portForward(ctx, clusterAddr)
	if err != nil {
		return clusterAddr, nil
	}
	return addr, nil
}

func (c *daemonConnector) portForward(ctx context.Context, clusterAddr string) (string, error) {
	c.mu.Lock()
	if pf, ok := c.forwards[clusterAddr]; ok {
		c.mu.Unlock()
		return pf.localAddr, nil
	}
	c.mu.Unlock()

	svcName, ns, remotePort, err := parseClusterSvcAddr(clusterAddr)
	if err != nil {
		return clusterAddr, err
	}

	podName, err := c.findDaemonPod(ctx, ns, svcName)
	if err != nil {
		return "", fmt.Errorf("find daemon pod for %s/%s: %w", ns, svcName, err)
	}

	localPort, err := freeLocalPort()
	if err != nil {
		return "", fmt.Errorf("allocate local port: %w", err)
	}

	stopCh := make(chan struct{}, 1)
	readyCh := make(chan struct{})
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", ns, podName)
	transport, upgrader, err := spdy.RoundTripperFor(c.restConfig)
	if err != nil {
		return "", fmt.Errorf("create port-forward transport: %w", err)
	}
	scheme, host := restConfigHost(c.restConfig)
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost,
		&url.URL{Scheme: scheme, Path: path, Host: host})
	fw, err := portforward.New(dialer, []string{fmt.Sprintf("%d:%d", localPort, remotePort)}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return "", fmt.Errorf("create port-forward: %w", err)
	}

	go func() {
		_ = fw.ForwardPorts()
	}()

	select {
	case <-readyCh:
	case <-ctx.Done():
		close(stopCh)
		return "", ctx.Err()
	}

	localAddr := fmt.Sprintf("127.0.0.1:%d", localPort)
	pf := &daemonPortForward{localAddr: localAddr, stopCh: stopCh}

	c.mu.Lock()
	c.forwards[clusterAddr] = pf
	c.mu.Unlock()

	return localAddr, nil
}

func (c *daemonConnector) findDaemonPod(ctx context.Context, ns, svcName string) (string, error) {
	svc, err := c.kubeClient.CoreV1().Services(ns).Get(ctx, svcName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if len(svc.Spec.Selector) == 0 {
		return "", fmt.Errorf("service %q has no selector", svcName)
	}
	pods, err := c.kubeClient.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set(svc.Spec.Selector).String(),
	})
	if err != nil {
		return "", err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodRunning {
			return pod.Name, nil
		}
	}
	return "", fmt.Errorf("no running daemon pod found for service %q", svcName)
}

// parseClusterSvcAddr parses addresses of the form "name.ns.svc:port".
func parseClusterSvcAddr(addr string) (svcName, ns string, port int, err error) {
	host, portStr, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		return "", "", 0, fmt.Errorf("invalid cluster service address %q: %w", addr, splitErr)
	}
	port, err = strconv.Atoi(portStr)
	if err != nil {
		return "", "", 0, fmt.Errorf("invalid port in address %q: %w", addr, err)
	}
	parts := strings.SplitN(host, ".", 3)
	if len(parts) != 3 || parts[2] != "svc" {
		return "", "", 0, fmt.Errorf("invalid cluster service address %q", addr)
	}
	return parts[0], parts[1], port, nil
}

func restConfigHost(config *rest.Config) (scheme, host string) {
	if strings.HasPrefix(config.Host, "https://") {
		return "https", strings.TrimPrefix(config.Host, "https://")
	}
	return "http", strings.TrimPrefix(config.Host, "http://")
}

func freeLocalPort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}
