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
	"strconv"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

type ReduceSuite struct {
	E2ESuite
}

// one reduce vertex (keyed)
func (r *ReduceSuite) TestSimpleKeyedReducePipeline() {
	w := r.Given().Pipeline("@testdata/simple-keyed-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().
		VertexPodsRunning()

	// port forward source vertex(to publish messages)
	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	// publish messages to source vertex, with event time starting from 60000
	startTime := 60000
	for i := 0; i < 300; i++ {
		eventTime := strconv.Itoa(startTime + i*1000)

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
	//w.Expect().VertexPodLogContains("sink", "20")
	w.Expect().VertexPodLogContains("sink", "Payload -  40  Key -  odd  Start -  60000  End -  70000")
	w.Expect().VertexPodLogContains("sink", "Payload -  20  Key -  even  Start -  60000  End -  70000")
}

// one reduce vertex(non keyed)
func (r *ReduceSuite) TestSimpleNonKeyedReducePipeline() {
	w := r.Given().Pipeline("@testdata/simple-non-keyed-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().
		VertexPodsRunning()

	// port forward source vertex(to publish messages)
	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	// publish messages to source vertex, with event time starting from 60000
	startTime := 60000
	for i := 0; i < 300; i++ {
		eventTime := strconv.Itoa(startTime + i*1000)

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
	w.Expect().VertexPodLogContains("sink", "Payload -  60  Key -  NON_KEYED_STREAM  Start -  60000  End -  70000")
}

// two reduce vertex(keyed and non keyed)
func (r *ReduceSuite) TestComplexReducePipelineKeyedNonKeyed() {
	w := r.Given().Pipeline("@testdata/complex-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().
		VertexPodsRunning()

	// port forward source vertex(to publish messages)
	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	// publish messages to source vertex, with event time starting from 60000
	startTime := 60000
	for i := 0; i < 300; i++ {
		eventTime := strconv.Itoa(startTime + i*1000)

		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime).
			Expect().
			Status(204)
	}

	// since the key can be even or odd and the first window duration is 10s(which is keyed)
	// and the second window duration is 60s(non-keyed)
	// the sum should be 180(60 + 120)
	w.Expect().VertexPodLogContains("sink", "Payload -  180  Key -  NON_KEYED_STREAM  Start -  120000  End -  180000")
}

func (r *ReduceSuite) TestSimpleReducePipelineFailOver() {
	w := r.Given().Pipeline("@testdata/simple-reduce-pipeline-wal.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "even-odd-sum"

	w.Expect().VertexPodsRunning()

	w.Expect().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("atoi", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("sink", LogSinkVertexStarted).
		DaemonPodLogContains(pipelineName, LogDaemonStarted)

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	args := "kubectl delete po -n numaflow-system -l " +
		"numaflow.numaproj.io/pipeline-name=even-odd-sum,numaflow.numaproj.io/vertex-name=compute-sum"

	// Kill the reducer pods before processing to trigger failover.
	w.Exec("/bin/sh", []string{"-c", args}, CheckPodKillSucceeded)

	startTime := int(time.Unix(1000, 0).UnixMilli())
	for i := 1; i <= 100; i++ {
		eventTime := startTime + (i * 1000)
		if i == 5 {
			// Kill the reducer pods during processing to trigger failover.
			w.Expect().VertexPodsRunning()
			w.Exec("/bin/sh", []string{"-c", args}, CheckPodKillSucceeded)
		}

		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("X-Numaflow-Event-Time", strconv.Itoa(eventTime)).WithBytes([]byte("1")).
			Expect().
			Status(204)

		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("X-Numaflow-Event-Time", strconv.Itoa(eventTime)).WithBytes([]byte("2")).
			Expect().
			Status(204)

		HTTPExpect(r.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("X-Numaflow-Event-Time", strconv.Itoa(eventTime)).WithBytes([]byte("3")).
			Expect().
			Status(204)
	}

	w.Expect().VertexPodLogContains("sink", "Payload -  38", PodLogCheckOptionWithCount(1))
	w.Expect().VertexPodLogContains("sink", "Payload -  76", PodLogCheckOptionWithCount(1))
	w.Expect().VertexPodLogContains("sink", "Payload -  120", PodLogCheckOptionWithCount(1))
	w.Expect().VertexPodLogContains("sink", "Payload -  240", PodLogCheckOptionWithCount(1))

	// Kill the reducer pods after processing to trigger failover.
	w.Exec("/bin/sh", []string{"-c", args}, CheckPodKillSucceeded)
	w.Expect().VertexPodsRunning()
	w.Expect().VertexPodLogContains("sink", "Payload -  38", PodLogCheckOptionWithCount(1))
	w.Expect().VertexPodLogContains("sink", "Payload -  76", PodLogCheckOptionWithCount(1))
	w.Expect().VertexPodLogContains("sink", "Payload -  120", PodLogCheckOptionWithCount(1))
	w.Expect().VertexPodLogContains("sink", "Payload -  240", PodLogCheckOptionWithCount(1))
}

func TestReduceSuite(t *testing.T) {
	suite.Run(t, new(ReduceSuite))
}
