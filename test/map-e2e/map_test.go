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
	"testing"

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
