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

package reduce_e2e

import (
	"fmt"
	"strconv"
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

type ReduceSuite struct {
	E2ESuite
}

func (r *ReduceSuite) TestSimpleKeyedReducePipeline() {
	w := r.Given().Pipeline("@testdata/simple-keyed-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("atoi", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("compute-sum", LogReduceUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("sink", LogSinkVertexStarted)

	// port forward source vertex(to publish messages)
	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	// publish messages to source vertex, with event time starting from 60000
	startTime := 60000
	for i := 0; i < 100; i++ {
		eventTime := strconv.Itoa(startTime + i*1000)
		fmt.Println("event time ", eventTime)

		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
	}

	// since the key can be even or odd and the window duration is 10s
	// the sum should be 20(for even) and 40(for odd)
	w.Expect().VertexPodLogContains("sink", "20")
	w.Expect().VertexPodLogContains("sink", "40")
	w.Expect().VertexPodLogContains("sink", "Start -  60000  End -  70000")
}

func (r *ReduceSuite) TestSimpleNonKeyedReducePipeline() {
	w := r.Given().Pipeline("@testdata/simple-non-keyed-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("compute-sum", LogReduceUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("sink", LogSinkVertexStarted)

	// port forward source vertex(to publish messages)
	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	// publish messages to source vertex, with event time starting from 60000
	startTime := 60000
	for i := 0; i < 300; i++ {
		eventTime := strconv.Itoa(startTime + i*1000)
		fmt.Println("event time ", eventTime)

		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
	}

	// since there is no key, all the messages will be assigned to same window
	// the sum should be 60(since the window is 10s)
	w.Expect().VertexPodLogContains("sink", "60")
	w.Expect().VertexPodLogContains("sink", "Start -  60000  End -  70000")
}

func (r *ReduceSuite) TestComplexReducePipeline() {
	w := r.Given().Pipeline("@testdata/complex-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("atoi", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("first-aggregation", LogReduceUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("second-aggregation", LogReduceUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("sink", LogSinkVertexStarted)

	// port forward source vertex(to publish messages)
	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	// publish messages to source vertex, with event time starting from 60000
	startTime := 60000
	for i := 0; i < 300; i++ {
		eventTime := strconv.Itoa(startTime + i*1000)
		fmt.Println("event time ", eventTime)

		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
	}

	// since the key can be even or odd and the first window duration is 10s(which is keyed)
	// and the second window duration is 60s(non keyed)
	// the sum should be 20(for even) and 40(for odd)
	w.Expect().VertexPodLogContains("sink", "360")
	w.Expect().VertexPodLogContains("sink", "Start -  60000  End -  120000")
}

func TestReduceSuite(t *testing.T) {
	suite.Run(t, new(ReduceSuite))
}
