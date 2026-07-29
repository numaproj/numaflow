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

package sdks_e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type MapSuite struct {
	E2ESuite
}

func (s *MapSuite) TestBatchMapUDFunctionAndSink() {
	w := s.Given().Pipeline("@testdata/flatmap-batch.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "flatmap-batch"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStartedRustRuntime).
		VertexPodLogContains("go-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("rust-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("rust-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello,hello"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello")))

	w.Expect().
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("rust-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("java-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3))
}

func (s *MapSuite) TestUDFunctionAndSink() {
	w := s.Given().Pipeline("@testdata/flatmap.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "flatmap"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStartedRustRuntime).
		VertexPodLogContains("go-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello,hello"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello")))

	w.Expect().
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("java-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3))
}

func (s *MapSuite) TestMapStreamUDFunctionAndSink() {
	w := s.Given().Pipeline("@testdata/flatmap-stream.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "flatmap-stream"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStartedRustRuntime).
		VertexPodLogContains("go-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello,hello,hello"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello")))

	w.Expect().
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
	w.Expect().
		VertexPodLogContains("go-udsink-2", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
	w.Expect().
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
	w.Expect().
		VertexPodLogContains("java-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
}

func (s *MapSuite) TestPipelineRuntimeErrorsFromUDFCrash() {
	w := s.Given().Pipeline("@testdata/runtime-error-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "runtime-error-pipeline"

	w.Expect().VertexPodsRunning().DaemonPodsRunning()

	defer w.VertexPodPortForward("p1", 8941, dfv1.VertexRuntimePort).
		DaemonPodPortForward(pipelineName, 1241, dfv1.DaemonServicePort).
		UXServerPodPortForward(8141, 8443).
		TerminateAllPodPortForwards()

	client, err := daemonclient.NewGRPCDaemonServiceClient("localhost:1241")
	assert.NoError(s.T(), err)
	defer func() { assert.NoError(s.T(), client.Close()) }()

	SendMessageTo(fmt.Sprintf("%s-in", pipelineName), "in", NewHttpPostRequest().WithBody([]byte("not-json")))

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8941").GET("/runtime/errors").
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udf"`)
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errors, err := client.GetVertexErrors(ctx, pipelineName, "p1")
		return err == nil && strings.Contains(fmt.Sprintf("%v", errors), "udf")
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8141").
			GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/%s/errors", Namespace, pipelineName, "p1")).
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udf"`)
	}, 2*time.Minute, time.Second)
}

func (s *MapSuite) TestPipelineRateLimitWithRedisStore() {
	w := s.Given().Pipeline("@testdata/rate-limit-redis.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "rate-limit-redis"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStartedRustRuntime).
		VertexPodLogContains("map-udf", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("out", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa"))

	// port-forward daemon server
	defer w.DaemonPodPortForward(pipelineName, 1234, dfv1.DaemonServicePort).
		TerminateAllPodPortForwards()

	// Verify rate limiting is working by checking processing rates
	client, err := daemonclient.NewGRPCDaemonServiceClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()

	w.Expect().VertexPodLogContains("map-udf", "processed\":\"50", PodLogCheckOptionWithContainer("numa"), PodLogCheckOptionWithCount(20))
}

func TestMapSuite(t *testing.T) {
	suite.Run(t, new(MapSuite))
}
