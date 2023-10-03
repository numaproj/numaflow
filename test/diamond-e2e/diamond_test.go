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

package e2e

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type DiamondSuite struct {
	E2ESuite
}

func (s *DiamondSuite) TestJoinSinkVertex() {
	w := s.Given().Pipeline("@testdata/join-on-sink.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "join-on-sink"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("888888"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("888889")))

	w.Expect().SinkContains("out", "888888")
	w.Expect().SinkContains("out", "888889")
}

func (s *DiamondSuite) TestCycleToSelf() {
	w := s.Given().Pipeline("@testdata/cycle-to-self.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "cycle-to-self"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	msgs := [10]string{}
	for i := 0; i < 10; i++ {
		msgs[i] = fmt.Sprintf("msg-%d", i)
		w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(msgs[i])))
	}
	for iteration := 1; iteration <= 3; iteration++ {
		for i := 0; i < 10; i++ {
			expectedString := fmt.Sprintf("count for \"msg-%d\"=%d", i, iteration)
			w.Expect().VertexPodLogContains("retry", expectedString, PodLogCheckOptionWithContainer("udf"))
		}
	}
	for i := 0; i < 10; i++ {
		w.Expect().SinkContains("out", msgs[i])
	}

}
func (s *DiamondSuite) TestCycleBackward() {
	w := s.Given().Pipeline("@testdata/cycle-backward.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "cycle-backward"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	msgs := [10]string{}
	for i := 0; i < 10; i++ {
		msgs[i] = fmt.Sprintf("msg-%d", i)
		w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(msgs[i])))
	}
	for iteration := 1; iteration <= 3; iteration++ {
		for i := 0; i < 10; i++ {
			expectedString := fmt.Sprintf("count for \"msg-%d\"=%d", i, iteration)
			w.Expect().VertexPodLogContains("retry", expectedString, PodLogCheckOptionWithContainer("udf"))
		}
	}
	for i := 0; i < 10; i++ {
		w.Expect().SinkContains("out", msgs[i])
	}
}

func TestDiamondSuite(t *testing.T) {
	suite.Run(t, new(DiamondSuite))
}
