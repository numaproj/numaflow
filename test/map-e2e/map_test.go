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

	"github.com/stretchr/testify/suite"

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
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("go-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("rust-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("rust-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa"))

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
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("go-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa"))

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
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("go-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa"))

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

func TestMapSuite(t *testing.T) {
	suite.Run(t, new(MapSuite))
}
