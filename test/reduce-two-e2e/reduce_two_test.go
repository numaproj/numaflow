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

package reduce_two_e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type ReduceSuite struct {
	E2ESuite
}

func (r *ReduceSuite) TestReduceStreamGo() {
	r.testReduceStream("go")
}

func (r *ReduceSuite) TestReduceStreamJava() {
	r.testReduceStream("java")
}

func (r *ReduceSuite) testReduceStream(lang string) {

	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		r.T().SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := r.Given().Pipeline(fmt.Sprintf("@testdata/reduce-stream/reduce-stream-%s.yaml", lang)).
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := fmt.Sprintf("reduce-stream-%s", lang)

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	defer w.StreamVertexPodlogs("sink", "udsink").TerminateAllPodLogs()

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
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime))
			}
		}
	}()

	// The reduce stream application summarizes the input messages and returns the sum when the sum is greater than 100.
	// Since we are sending 3s, the first returned message should be 102.
	// There should be no other values.
	w.Expect().SinkContains("sink", "102")
	w.Expect().SinkNotContains("sink", "99")
	w.Expect().SinkNotContains("sink", "105")
	done <- struct{}{}
}

func (r *ReduceSuite) TestSimpleSessionPipeline() {

	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		r.T().SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := r.Given().Pipeline("@testdata/session-reduce/simple-session-sum-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "simple-session-sum"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	count := 0
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
				if count == 10 {
					startTime = startTime + (10 * 1000)
					count = 0
				} else {
					startTime = startTime + 1000
				}
				eventTime := strconv.Itoa(startTime)
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("100")).WithHeader("X-Numaflow-Event-Time", eventTime))
				count += 1
			}
		}
	}()

	w.Expect().SinkContains("sink", "1000")
	done <- struct{}{}
}

func (r *ReduceSuite) TestSimpleSessionKeyedPipelineGo() {
	r.testSimpleSessionKeyedPipeline("go")
}

func (r *ReduceSuite) TestSimpleSessionKeyedPipelineJava() {
	r.testSimpleSessionKeyedPipeline("java")
}

func (r *ReduceSuite) testSimpleSessionKeyedPipeline(lang string) {

	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		r.T().SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := r.Given().Pipeline(fmt.Sprintf("@testdata/session-reduce/simple-session-keyed-counter-pipeline-%s.yaml", lang)).
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := fmt.Sprintf("simple-session-counter-%s", lang)

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	defer w.StreamVertexPodlogs("sink", "udsink").TerminateAllPodLogs()

	count := 0
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
				if count == 10 {
					startTime = startTime + (10 * 1000)
					count = 0
				} else {
					startTime = startTime + 1000
				}
				eventTime := strconv.Itoa(startTime)
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime))
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime))
				count += 2
			}
		}
	}()

	w.Expect().SinkContains("sink", "5")
	w.Expect().SinkNotContains("sink", "4", SinkCheckWithTimeout(20*time.Second))
	w.Expect().SinkNotContains("sink", "3", SinkCheckWithTimeout(20*time.Second))
	w.Expect().SinkNotContains("sink", "2", SinkCheckWithTimeout(20*time.Second))
	w.Expect().SinkNotContains("sink", "1", SinkCheckWithTimeout(20*time.Second))
	done <- struct{}{}
}

func (r *ReduceSuite) TestSimpleSessionPipelineFailOverUsingWAL() {

	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		r.T().SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := r.Given().Pipeline("@testdata/session-reduce/simple-session-keyed-counter-pipeline-go.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "simple-session-counter-go"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	args := "kubectl delete po -n numaflow-system -l " +
		"numaflow.numaproj.io/pipeline-name=simple-session-counter-go,numaflow.numaproj.io/vertex-name=compute-count"

	// Kill the reducer pods before processing to trigger failover.
	w.Exec("/bin/sh", []string{"-c", args}, CheckPodKillSucceeded)
	done := make(chan struct{})
	count := 0
	go func() {
		startTime := 0
		for i := 1; true; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				if count == 10 {
					startTime = startTime + (10 * 1000)
					count = 0
				} else {
					startTime = startTime + 1000
				}
				eventTime := strconv.Itoa(startTime)
				if i == 5 {
					// Kill the reducer pods during processing to trigger failover.
					w.Expect().VertexPodsRunning()
					w.Exec("/bin/sh", []string{"-c", args}, CheckPodKillSucceeded)
				}
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime))
				count += 2
			}
		}
	}()

	w.Expect().
		SinkContains("sink", "5").
		SinkNotContains("sink", "4", SinkCheckWithTimeout(20*time.Second)).
		SinkNotContains("sink", "3", SinkCheckWithTimeout(20*time.Second)).
		SinkNotContains("sink", "2", SinkCheckWithTimeout(20*time.Second)).
		SinkNotContains("sink", "1", SinkCheckWithTimeout(20*time.Second))
	done <- struct{}{}
}

func TestSessionSuite(t *testing.T) {
	suite.Run(t, new(ReduceSuite))
}
