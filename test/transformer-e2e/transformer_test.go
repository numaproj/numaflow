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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type TransformerSuite struct {
	E2ESuite
}

func (s *TransformerSuite) TestSourceFiltering() {
	w := s.Given().Pipeline("@testdata/source-filtering.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "source-filtering"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	expect0 := `{"id": 180, "msg": "hello", "expect0": "fail", "desc": "A bad example"}`
	expect1 := `{"id": 80, "msg": "hello1", "expect1": "fail", "desc": "A bad example"}`
	expect2 := `{"id": 80, "msg": "hello", "expect2": "fail", "desc": "A bad example"}`
	expect3 := `{"id": 80, "msg": "hello", "expect3": "succeed", "desc": "A good example"}`
	expect4 := `{"id": 80, "msg": "hello", "expect4": "succeed", "desc": "A good example"}`

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect0))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect1))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect2))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect3))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect4)))

	w.Expect().SinkContains("out", expect3)
	w.Expect().SinkContains("out", expect4)
	w.Expect().SinkNotContains("out", expect0)
	w.Expect().SinkNotContains("out", expect1)
	w.Expect().SinkNotContains("out", expect2)
}

func (s *TransformerSuite) TestTimeExtractionFilter() {
	w := s.Given().Pipeline("@testdata/time-extraction-filter.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "time-extraction-filter"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	testMsgOne := `{"id": 80, "msg": "hello", "time": "2021-01-18T21:54:42.123Z", "desc": "A good ID."}`
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgOne)))
	w.Expect().VertexPodLogContains("out", fmt.Sprintf("EventTime -  %d", time.Date(2021, 1, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli()), PodLogCheckOptionWithCount(1), PodLogCheckOptionWithContainer("numa"))

	testMsgTwo := `{"id": 101, "msg": "test", "time": "2021-01-18T21:54:42.123Z", "desc": "A bad ID."}`
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsgTwo)))
	w.Expect().SinkNotContains("out", testMsgTwo)
}

func (s *TransformerSuite) TestBuiltinEventTimeExtractor() {

	// this test is skipped for redis as watermark is not supported with this ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		s.T().SkipNow()
	}

	w := s.Given().Pipeline("@testdata/extract-event-time-from-payload.yaml").
		When().
		CreatePipelineAndWait()
	currentTime := time.Now().UnixMilli()
	defer w.DeletePipelineAndWait()
	pipelineName := "extract-event-time"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning().DaemonPodsRunning()

	defer w.DaemonPodPortForward(pipelineName, 1234, dfv1.DaemonServicePort).
		TerminateAllPodPortForwards()

	// Use the daemon client to verify watermark propagation.
	client, err := daemonclient.NewGRPCDaemonServiceClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		startTime := time.Date(2021, 1, 18, 21, 54, 42, 123000000, time.UTC)
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			default:
				testMsg := generateTestMsg("numa", startTime)
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(testMsg)))
				startTime = startTime.Add(1 * time.Minute)
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
	// In this test, we send a message with event time being now, apply event time extractor and verify from log that the message event time gets updated.
	w.Expect().VertexPodLogContains("out", fmt.Sprintf("EventTime -  %d", time.Date(2021, 1, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli()), PodLogCheckOptionWithCount(1))

wmLoop:
	for {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				s.T().Log("test timed out")
				assert.Fail(s.T(), "timed out")
				break wmLoop
			}
		default:
			wm, err := client.GetPipelineWatermarks(ctx, pipelineName)
			edgeWM := wm[0].Watermarks[0]
			if wm[0].Watermarks[0].GetValue() != -1 {
				assert.NoError(s.T(), err)
				if err != nil {
					assert.Fail(s.T(), err.Error())
				}
				// Watermark propagation can delay, we consider the test as passed as long as the retrieved watermark is greater than the event time of the first message
				// and less than the current time.
				assert.True(s.T(), edgeWM.GetValue() >= time.Date(2021, 1, 18, 21, 54, 42, 123000000, time.UTC).UnixMilli() && edgeWM.GetValue() < currentTime)
				break wmLoop
			}
			time.Sleep(time.Second)
		}
	}
	done <- struct{}{}
}

func TestTransformerSuite(t *testing.T) {
	suite.Run(t, new(TransformerSuite))
}

type Item struct {
	ID   int       `json:"id"`
	Name string    `json:"name"`
	Time time.Time `json:"time"`
}

type TestMsg struct {
	Test int    `json:"test"`
	Item []Item `json:"item"`
}

func generateTestMsg(msg string, t time.Time) string {
	items := []Item{
		{ID: 1, Name: msg, Time: t},
		{ID: 2, Name: msg, Time: t},
	}
	testMsg := TestMsg{Test: 21, Item: items}
	jsonBytes, _ := json.Marshal(testMsg)
	return string(jsonBytes)
}
