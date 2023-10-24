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

package sideinput_e2e

import (
	"context"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
	"strconv"
	"testing"
	"time"
)

type SideInputUDSSuite struct {
	E2ESuite
}

func (s *SideInputUDSSuite) TestUserDefinedSinkWithSideInput() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := s.Given().Pipeline("@testdata/sideinput_sink.yaml").When().CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	w.Expect().VertexPodsRunning()
	pipelineName := "sideinput-sink-test"
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
	w.Expect().SinkContains("redis-uds", "e2e-even", WithTimeout(5*time.Minute))
	done <- struct{}{}
}

func (s *SideInputUDSSuite) TestSourceWithSideInput() {
	w := s.Given().Pipeline("@testdata/sideinput_source.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()
	w.Expect().SinkContains("redis-uds", "e2e-even", WithTimeout(5*time.Minute))

}

func TestSideInputUDSSuite(t *testing.T) {
	suite.Run(t, new(SideInputUDSSuite))
}
