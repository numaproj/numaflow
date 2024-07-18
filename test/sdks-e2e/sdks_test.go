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
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type SDKsSuite struct {
	E2ESuite
}

func (s *SDKsSuite) TestUDFunctionAndSink() {
	w := s.Given().Pipeline("@testdata/flatmap.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "flatmap"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("go-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello,hello"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello")))

	w.Expect().
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("java-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3))
}

func (s *SDKsSuite) TestMapStreamUDFunctionAndSink() {
	w := s.Given().Pipeline("@testdata/flatmap-stream.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "flatmap-stream"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("go-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa"))
	//VertexPodLogContains("java-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
	//	VertexPodLogContains("java-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello,hello,hello"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello")))

	w.Expect().
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
	w.Expect().
		VertexPodLogContains("go-udsink-2", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
	w.Expect().
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))

	// FIXME(map-batch): enable Java
	//w.Expect().
	//	VertexPodLogContains("java-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
}

func (s *SDKsSuite) TestBatchMapUDFunctionAndSink() {
	w := s.Given().Pipeline("@testdata/flatmap-batch.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "flatmap-batch"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("go-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa"))
	// FIXME(map-batch): enable Java
	//VertexPodLogContains("java-split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
	//	VertexPodLogContains("java-udsink", SinkVertexStarted, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello,hello"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello")))

	w.Expect().
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3))
	// FIXME(map-batch): enable Java
	//VertexPodLogContains("java-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
}

func (s *SDKsSuite) TestReduceSDK() {

	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		s.T().SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := s.Given().Pipeline("@testdata/simple-keyed-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "even-odd-sum"

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

	w.Expect().
		VertexPodLogContains("java-udsink", "120", PodLogCheckOptionWithContainer("udsink")).
		VertexPodLogContains("java-udsink", "240", PodLogCheckOptionWithContainer("udsink"))
	done <- struct{}{}
}

func (s *SDKsSuite) TestSourceTransformer() {

	// the transformer feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		s.T().SkipNow()
	}

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		s.testSourceTransformer("python")
	}()
	go func() {
		defer wg.Done()
		s.testSourceTransformer("java")
	}()
	go func() {
		defer wg.Done()
		s.testSourceTransformer("go")
	}()
	go func() {
		defer wg.Done()
		s.testSourceTransformer("rust")
	}()
	wg.Wait()
}

func (s *SDKsSuite) testSourceTransformer(lang string) {
	w := s.Given().Pipeline(fmt.Sprintf("@testdata/transformer/event-time-filter-%s.yaml", lang)).
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := fmt.Sprintf("event-time-filter-%s", lang)

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	eventTimeBefore2022_1 := strconv.FormatInt(time.Date(2021, 4, 2, 7, 4, 5, 2, time.UTC).UnixMilli(), 10)
	eventTimeBefore2022_2 := strconv.FormatInt(time.Date(1998, 4, 2, 8, 4, 5, 2, time.UTC).UnixMilli(), 10)
	eventTimeBefore2022_3 := strconv.FormatInt(time.Date(2013, 4, 4, 7, 4, 5, 2, time.UTC).UnixMilli(), 10)

	eventTimeAfter2022_1 := strconv.FormatInt(time.Date(2023, 4, 2, 7, 4, 5, 2, time.UTC).UnixMilli(), 10)
	eventTimeAfter2022_2 := strconv.FormatInt(time.Date(2026, 4, 2, 3, 4, 5, 2, time.UTC).UnixMilli(), 10)

	eventTimeWithin2022_1 := strconv.FormatInt(time.Date(2022, 4, 2, 3, 4, 5, 2, time.UTC).UnixMilli(), 10)

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("Before2022")).WithHeader("X-Numaflow-Event-Time", eventTimeBefore2022_1)).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("Before2022")).WithHeader("X-Numaflow-Event-Time", eventTimeBefore2022_2)).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("Before2022")).WithHeader("X-Numaflow-Event-Time", eventTimeBefore2022_3)).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("After2022")).WithHeader("X-Numaflow-Event-Time", eventTimeAfter2022_1)).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("After2022")).WithHeader("X-Numaflow-Event-Time", eventTimeAfter2022_2)).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("Within2022")).WithHeader("X-Numaflow-Event-Time", eventTimeWithin2022_1))

	janFirst2022 := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	janFirst2023 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)

	w.Expect().VertexPodLogContains("sink-within-2022", fmt.Sprintf("EventTime -  %d", janFirst2022.UnixMilli()), PodLogCheckOptionWithCount(1)).
		VertexPodLogContains("sink-after-2022", fmt.Sprintf("EventTime -  %d", janFirst2023.UnixMilli()), PodLogCheckOptionWithCount(2)).
		VertexPodLogContains("sink-all", fmt.Sprintf("EventTime -  %d", janFirst2022.UnixMilli()), PodLogCheckOptionWithCount(1)).
		VertexPodLogContains("sink-all", fmt.Sprintf("EventTime -  %d", janFirst2023.UnixMilli()), PodLogCheckOptionWithCount(2)).
		VertexPodLogNotContains("sink-within-2022", fmt.Sprintf("EventTime -  %d", janFirst2023.UnixMilli()), PodLogCheckOptionWithTimeout(1*time.Second)).
		VertexPodLogNotContains("sink-after-2022", fmt.Sprintf("EventTime -  %d", janFirst2022.UnixMilli()), PodLogCheckOptionWithTimeout(1*time.Second)).
		VertexPodLogNotContains("sink-all", "Before2022", PodLogCheckOptionWithTimeout(1*time.Second)).
		VertexPodLogNotContains("sink-within-2022", "Before2022", PodLogCheckOptionWithTimeout(1*time.Second)).
		VertexPodLogNotContains("sink-after-2022", "Before2022", PodLogCheckOptionWithTimeout(1*time.Second))
}

func TestHTTPSuite(t *testing.T) {
	suite.Run(t, new(SDKsSuite))
}
