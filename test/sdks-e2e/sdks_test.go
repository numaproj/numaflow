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

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
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

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("go-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", LogSinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", LogSinkVertexStarted, PodLogCheckOptionWithContainer("numa"))

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("hello,hello")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("hello")).
		Expect().
		Status(204)

	w.Expect().VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3))
}

func TestHTTPSuite(t *testing.T) {
	suite.Run(t, new(SDKsSuite))
}
