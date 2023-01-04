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
	"strconv"
	"testing"
	"time"

	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

type SDKsSuite struct {
	E2ESuite
}

func (s *SDKsSuite) TestUDFunctionAndSink() {
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
		VertexPodLogContains("python-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", *NewRequestBuilder().WithBody([]byte("hello,hello")).Build()).
		SendMessageTo(pipelineName, "in", *NewRequestBuilder().WithBody([]byte("hello")).Build())

	w.Expect().VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3))
}

func (s *SDKsSuite) TestJavaSdk() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := s.Given().Pipeline("@testdata/simple-keyed-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "even-odd-sum"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	done := make(chan struct{})
	go func() {
		// publish messages to source vertex, with event time starting from 60000
		startTime := 60000
		for i := 0; true; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				eventTime := strconv.Itoa(startTime + i*1000)
				w.SendMessageTo(pipelineName, "in", *NewRequestBuilder().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime).Build()).
					SendMessageTo(pipelineName, "in", *NewRequestBuilder().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime).Build()).
					SendMessageTo(pipelineName, "in", *NewRequestBuilder().WithBody([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime).Build())
			}
		}
	}()

	w.Expect().
		VertexPodLogContains("java-udsink", "120", PodLogCheckOptionWithContainer("udsink")).
		VertexPodLogContains("java-udsink", "120", PodLogCheckOptionWithContainer("udsink"))
	done <- struct{}{}
}

func TestHTTPSuite(t *testing.T) {
	suite.Run(t, new(SDKsSuite))
}
