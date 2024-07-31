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

package sideinput_e2e

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type SideInputSuite struct {
	E2ESuite
}

func (s *SideInputSuite) TestSimpleMapSideInputPipeline() {

	// the side inputs feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		s.T().SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := s.Given().Pipeline("@testdata/map-sideinput-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "map-sideinput-pipeline"

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
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("map")).WithHeader("X-Numaflow-Event-Time", eventTime))
			}
		}
	}()

	// map-even-data and map-odd-data message is generated based on map and side input data.
	w.Expect().SinkContains("sink", "map-even-data")
	w.Expect().SinkContains("sink", "map-odd-data")

	done <- struct{}{}
}

func (s *SideInputSuite) TestSimpleReduceSideInputPipeline() {

	// the side inputs feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		s.T().SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := s.Given().Pipeline("@testdata/reduce-sideinput-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "reduce-sideinput-pipeline"

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
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("x-numaflow-event-time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("x-numaflow-event-time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("3")).WithHeader("x-numaflow-event-time", eventTime))
			}
		}
	}()

	// here reduce-side-input text is generated based on reduce and side input data.
	w.Expect().SinkContains("sink", "reduce_sideinput")

	done <- struct{}{}
}

func TestSideInputSuite(t *testing.T) {
	suite.Run(t, new(SideInputSuite))
}
