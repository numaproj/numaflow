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

func (s *ReduceSuite) TestSimpleReducePipelineFailOver() {
	w := s.Given().Pipeline("@testdata/simple-reduce-pipeline.yaml").
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

		HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("X-Numaflow-Event-Time", strconv.Itoa(eventTime)).WithBytes([]byte("1")).
			Expect().
			Status(204)

		HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("X-Numaflow-Event-Time", strconv.Itoa(eventTime)).WithBytes([]byte("2")).
			Expect().
			Status(204)

		HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("X-Numaflow-Event-Time", strconv.Itoa(eventTime)).WithBytes([]byte("3")).
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
