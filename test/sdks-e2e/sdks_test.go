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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type SDKsSuite struct {
	E2ESuite
}

func (s *SDKsSuite) TestSourceTransformerPython() {
	w := s.Given().Pipeline("@testdata/transformer/event-time-filter-python.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "event-time-filter-python"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	eventTimeBefore2022_1 := strconv.FormatInt(time.Date(2021, 4, 2, 7, 4, 5, 2, time.UTC).UnixMilli(), 10)
	eventTimeBefore2022_2 := strconv.FormatInt(time.Date(1998, 4, 2, 8, 4, 5, 2, time.UTC).UnixMilli(), 10)
	eventTimeBefore2022_3 := strconv.FormatInt(time.Date(2013, 4, 4, 7, 4, 5, 2, time.UTC).UnixMilli(), 10)

	eventTimeAfter2022_1 := strconv.FormatInt(time.Date(2023, 4, 2, 7, 4, 5, 2, time.UTC).UnixMilli(), 10)
	eventTimeAfter2022_2 := strconv.FormatInt(time.Date(2026, 4, 2, 3, 4, 5, 2, time.UTC).UnixMilli(), 10)

	eventTimeWithin2022_1 := strconv.FormatInt(time.Date(2022, 4, 2, 3, 4, 5, 2, time.UTC).UnixMilli(), 10)

	go func() {
		w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("Before2022")).WithHeader("X-Numaflow-Event-Time", eventTimeBefore2022_1))
		time.Sleep(time.Minute)
		w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("Before2022")).WithHeader("X-Numaflow-Event-Time", eventTimeBefore2022_2)).
			SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("Before2022")).WithHeader("X-Numaflow-Event-Time", eventTimeBefore2022_3)).
			SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("After2022")).WithHeader("X-Numaflow-Event-Time", eventTimeAfter2022_1)).
			SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("After2022")).WithHeader("X-Numaflow-Event-Time", eventTimeAfter2022_2)).
			SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("Within2022")).WithHeader("X-Numaflow-Event-Time", eventTimeWithin2022_1))
	}()

	janFirst2022 := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	// janFirst2023 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	w.Expect().VertexPodLogContains("in", fmt.Sprintf("EventTime -  %d", janFirst2022.UnixMilli()), PodLogCheckOptionWithCount(1), PodLogCheckOptionWithContainer("numa"))
}

func TestHTTPSuite(t *testing.T) {
	suite.Run(t, new(SDKsSuite))
}
