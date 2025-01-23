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

package monovertex_e2e

import (
	"testing"

	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type MonoVertexSuite struct {
	E2ESuite
}

func (s *MonoVertexSuite) TestMonoVertexWithAllContainers() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-with-all-containers.yaml").
		When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().MonoVertexPodsRunning().MvtxDaemonPodsRunning()

	defer w.MonoVertexPodPortForward(8931, dfv1.MonoVertexMetricsPort).
		MvtxDaemonPodPortForward(3232, dfv1.MonoVertexDaemonServicePort).
		TerminateAllPodPortForwards()

	// Check metrics endpoints
	HTTPExpect(s.T(), "https://localhost:8931").GET("/metrics").
		Expect().
		Status(200)

	HTTPExpect(s.T(), "https://localhost:3232").GET("/metrics").
		Expect().
		Status(200)

	// Expect the messages to be processed by the transformer.
	w.Expect().MonoVertexPodLogContains("AssignEventTime", PodLogCheckOptionWithContainer("transformer"))

	// Simulate primary sink failure and check fallback sink
	w.Expect().MonoVertexPodLogContains("Primary sink under maintenance", PodLogCheckOptionWithContainer("udsink"))

	// Expect the messages to reach the fallback sink.
	w.Expect().RedisSinkContains("fallback-transformer-mono-vertex", "1000")
	w.Expect().RedisSinkContains("fallback-transformer-mono-vertex", "1001")

}

func TestMonoVertexSuite(t *testing.T) {
	suite.Run(t, new(MonoVertexSuite))
}
