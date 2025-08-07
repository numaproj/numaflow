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
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type DiamondSuite struct {
	E2ESuite
}

func (s *DiamondSuite) TestJoinOnReducePipeline() {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := s.Given().Pipeline("@testdata/join-on-reduce.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "join-on-reduce"

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
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime))
			}
		}
	}()
	// todo: this only tests for one occurrence: ideally should verify all
	w.Expect().
		RedisSinkContains("join-on-reduce-sink", "40"). // per 10-second window: (10 * 2) * 2 atoi vertices
		RedisSinkContains("join-on-reduce-sink", "80")  // per 10-second window: 10 * (1 + 3) * 2 atoi vertices
	done <- struct{}{}
}

func (s *DiamondSuite) TestJoinOnMapPipeline() {
	w := s.Given().Pipeline("@testdata/join-on-map.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "join-on-map"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	w.SendMessageTo(pipelineName, "in-0", NewHttpPostRequest().WithBody([]byte("1")))
	w.SendMessageTo(pipelineName, "in-1", NewHttpPostRequest().WithBody([]byte("2")))

	w.Expect().
		RedisSinkContains("join-on-map-sink", "1").
		RedisSinkContains("join-on-map-sink", "2")
}

func (s *DiamondSuite) TestJoinOnSinkVertex() {
	w := s.Given().Pipeline("@testdata/join-on-sink.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "join-on-sink"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("888888"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("888889")))

	w.Expect().RedisSinkContains("join-on-sink-out", "888888")
	w.Expect().RedisSinkContains("join-on-sink-out", "888889")
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
		w.Expect().RedisSinkContains("cycle-to-self-out", msgs[i])
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
		w.Expect().RedisSinkContains("cycle-backward-out", msgs[i])
	}
}

func TestDiamondSuite(t *testing.T) {
	suite.Run(t, new(DiamondSuite))
}
