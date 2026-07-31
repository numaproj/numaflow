//go:build test

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

package redrive_e2e

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonpb "github.com/numaproj/numaflow/pkg/apis/proto/daemon"
	mvtxdaemonpb "github.com/numaproj/numaflow/pkg/apis/proto/mvtxdaemon"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtxclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type RedriveSuite struct {
	E2ESuite
}

var runtimeErrorHTTPClient = &http.Client{
	Timeout:   5 * time.Second,
	Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
}

// httpBodyContains is used inside assert.Eventually, so transient HTTP failures
// should return false instead of failing the test before the next poll. Every miss is logged, as
// InvokeE2EAPI does for its retries, otherwise a timeout gives no clue about which poll kept failing.
func httpBodyContains(baseURL, path, expected string) bool {
	url := baseURL + path
	resp, err := runtimeErrorHTTPClient.Get(url)
	if err != nil {
		log.Printf("GET %s failed: %v, retrying.\n", url, err)
		return false
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		log.Printf("GET %s returned %s, body: %s, retrying.\n", url, resp.Status, body)
		return false
	}
	if readErr != nil {
		log.Printf("GET %s body read failed: %v, retrying.\n", url, readErr)
		return false
	}
	if !strings.Contains(string(body), expected) {
		log.Printf("GET %s does not contain %q yet, body: %s, retrying.\n", url, expected, body)
		return false
	}
	return true
}

// vertexHasContainerError reports whether any replica recorded a runtime error for the given container.
func vertexHasContainerError(replicaErrors []*daemonpb.ReplicaErrors, container string) bool {
	for _, re := range replicaErrors {
		for _, ce := range re.GetContainerErrors() {
			if ce.GetContainer() == container {
				return true
			}
		}
	}
	return false
}

// monoVertexHasContainerError reports whether any replica recorded a runtime error for the given container.
func monoVertexHasContainerError(replicaErrors []*mvtxdaemonpb.ReplicaErrors, container string) bool {
	for _, re := range replicaErrors {
		for _, ce := range re.GetContainerErrors() {
			if ce.GetContainer() == container {
				return true
			}
		}
	}
	return false
}

func (s *RedriveSuite) TestPipelineRuntimeErrorsFromUDFCrash() {
	w := s.Given().Pipeline("@testdata/runtime-error-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "runtime-error-pipeline"

	w.Expect().VertexPodsRunning().DaemonPodsRunning()
	podSnapshot := w.Expect().VertexPodRuntimeSnapshot("p1")

	defer w.VertexPodPortForward("p1", 8941, dfv1.VertexRuntimePort).
		DaemonPodPortForward(pipelineName, 1241, dfv1.DaemonServicePort).
		UXServerPodPortForward(8141, 8443).
		TerminateAllPodPortForwards()

	client, err := daemonclient.NewGRPCDaemonServiceClient("localhost:1241")
	assert.NoError(s.T(), err)
	defer func() { assert.NoError(s.T(), client.Close()) }()

	SendMessageTo(fmt.Sprintf("%s-in", pipelineName), "in", NewHttpPostRequest().WithBody([]byte("not-json")))

	assert.Eventually(s.T(), func() bool {
		return httpBodyContains("https://localhost:8941", "/runtime/errors", `"container":"udf"`)
	}, 2*time.Minute, time.Second, "udf runtime error not reported by the p1 pod runtime endpoint")

	assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		replicaErrors, err := client.GetVertexErrors(ctx, pipelineName, "p1")
		return err == nil && vertexHasContainerError(replicaErrors, "udf")
	}, 2*time.Minute, time.Second, "udf runtime error not reported by the daemon for vertex p1")

	assert.Eventually(s.T(), func() bool {
		return httpBodyContains(
			"https://localhost:8141",
			fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/%s/errors", Namespace, pipelineName, "p1"),
			`"container":"udf"`,
		)
	}, 2*time.Minute, time.Second, "udf runtime error not reported by the UX server for vertex p1")

	w.Expect().VertexNumaStable(podSnapshot, "p1")
}

func (s *RedriveSuite) TestPipelineRuntimeErrorsFromSinkCrash() {
	w := s.Given().Pipeline("@testdata/runtime-error-sink-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "runtime-error-sink-pipeline"

	podSnapshot := w.Expect().VertexPodRuntimeSnapshot("out")
	w.Expect().DaemonPodsRunning()

	defer w.VertexPodPortForward("out", 8942, dfv1.VertexRuntimePort).
		DaemonPodPortForward(pipelineName, 1242, dfv1.DaemonServicePort).
		UXServerPodPortForward(8142, 8443).
		TerminateAllPodPortForwards()

	client, err := daemonclient.NewGRPCDaemonServiceClient("localhost:1242")
	assert.NoError(s.T(), err)
	defer func() { assert.NoError(s.T(), client.Close()) }()

	assert.Eventually(s.T(), func() bool {
		return httpBodyContains("https://localhost:8942", "/runtime/errors", `"container":"udsink"`)
	}, 2*time.Minute, time.Second, "udsink runtime error not reported by the out pod runtime endpoint")

	assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		replicaErrors, err := client.GetVertexErrors(ctx, pipelineName, "out")
		return err == nil && vertexHasContainerError(replicaErrors, "udsink")
	}, 2*time.Minute, time.Second, "udsink runtime error not reported by the daemon for vertex out")

	assert.Eventually(s.T(), func() bool {
		return httpBodyContains(
			"https://localhost:8142",
			fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/%s/errors", Namespace, pipelineName, "out"),
			`"container":"udsink"`,
		)
	}, 2*time.Minute, time.Second, "udsink runtime error not reported by the UX server for vertex out")

	w.Expect().VertexNumaStable(podSnapshot, "out")
}

func (s *RedriveSuite) TestMonoVertexRuntimeErrorsFromUDFCrash() {
	monoVertexName := "runtime-error-monovertex"
	w := s.Given().MonoVertex("@testdata/runtime-error-monovertex.yaml").
		When().CreateMonoVertexAndWait()
	defer w.Exec("kubectl", []string{"delete", "monovertices.numaflow.numaproj.io", monoVertexName, "-n", Namespace, "--ignore-not-found=true"}, OutputRegexp(""))

	w.Expect().MonoVertexPodsRunning().MvtxDaemonPodsRunning()
	podSnapshot := w.Expect().MonoVertexPodRuntimeSnapshot()

	defer w.MonoVertexPodPortForward(8943, dfv1.MonoVertexRuntimePort).
		MvtxDaemonPodPortForward(3243, dfv1.MonoVertexDaemonServicePort).
		UXServerPodPortForward(8143, 8443).
		TerminateAllPodPortForwards()

	client, err := mvtxclient.NewGRPCClient("localhost:3243")
	assert.NoError(s.T(), err)
	defer func() { assert.NoError(s.T(), client.Close()) }()

	go func() {
		defer func() {
			// The HTTP request can outlive the test because the UDF process exits before ACKing it.
			_ = recover()
		}()
		SendMessageTo(monoVertexName, monoVertexName, NewHttpPostRequest().WithBody([]byte("not-json")))
	}()

	assert.Eventually(s.T(), func() bool {
		return httpBodyContains("https://localhost:8943", "/runtime/errors", `"container":"udf"`)
	}, 2*time.Minute, time.Second, "udf runtime error not reported by the monovertex pod runtime endpoint")

	assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		replicaErrors, err := client.GetMonoVertexErrors(ctx, monoVertexName)
		return err == nil && monoVertexHasContainerError(replicaErrors, "udf")
	}, 2*time.Minute, time.Second, "udf runtime error not reported by the monovertex daemon")

	assert.Eventually(s.T(), func() bool {
		return httpBodyContains(
			"https://localhost:8143",
			fmt.Sprintf("/api/v1/namespaces/%s/mono-vertices/%s/errors", Namespace, monoVertexName),
			`"container":"udf"`,
		)
	}, 2*time.Minute, time.Second, "udf runtime error not reported by the UX server for the monovertex")

	w.Expect().MonoVertexNumaStable(podSnapshot)
}

func (s *RedriveSuite) TestMonoVertexRuntimeErrorsFromSinkCrash() {
	monoVertexName := "runtime-error-sink-monovertex"
	w := s.Given().MonoVertex("@testdata/runtime-error-sink-monovertex.yaml").
		When().CreateMonoVertexAndWait()
	defer w.Exec("kubectl", []string{"delete", "monovertices.numaflow.numaproj.io", monoVertexName, "-n", Namespace, "--ignore-not-found=true"}, OutputRegexp(""))

	podSnapshot := w.Expect().MonoVertexPodRuntimeSnapshot()
	w.Expect().MvtxDaemonPodsRunning()

	defer w.MonoVertexPodPortForward(8944, dfv1.MonoVertexRuntimePort).
		MvtxDaemonPodPortForward(3244, dfv1.MonoVertexDaemonServicePort).
		UXServerPodPortForward(8144, 8443).
		TerminateAllPodPortForwards()

	client, err := mvtxclient.NewGRPCClient("localhost:3244")
	assert.NoError(s.T(), err)
	defer func() { assert.NoError(s.T(), client.Close()) }()

	assert.Eventually(s.T(), func() bool {
		return httpBodyContains("https://localhost:8944", "/runtime/errors", `"container":"udsink"`)
	}, 2*time.Minute, time.Second, "udsink runtime error not reported by the monovertex pod runtime endpoint")

	assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		replicaErrors, err := client.GetMonoVertexErrors(ctx, monoVertexName)
		return err == nil && monoVertexHasContainerError(replicaErrors, "udsink")
	}, 2*time.Minute, time.Second, "udsink runtime error not reported by the monovertex daemon")

	assert.Eventually(s.T(), func() bool {
		return httpBodyContains(
			"https://localhost:8144",
			fmt.Sprintf("/api/v1/namespaces/%s/mono-vertices/%s/errors", Namespace, monoVertexName),
			`"container":"udsink"`,
		)
	}, 2*time.Minute, time.Second, "udsink runtime error not reported by the UX server for the monovertex")

	w.Expect().MonoVertexNumaStable(podSnapshot)
}

func TestRedriveSuite(t *testing.T) {
	suite.Run(t, new(RedriveSuite))
}
