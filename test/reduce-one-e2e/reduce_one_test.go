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

package reduce_one_e2e

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

type ReduceSuite struct {
	E2ESuite
}

// one reduce vertex (keyed)
func (r *ReduceSuite) TestSimpleKeyedReducePipeline() {
	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		r.T().SkipNow()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	w := r.Given().Pipeline("@testdata/simple-keyed-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "simple-sum"

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

	// since the key can be even or odd and the window duration is 10s
	// the sum should be 20(for even) and 40(for odd)
	w.Expect().
		RedisSinkContains("simple-sum-sink", "40").
		RedisSinkContains("simple-sum-sink", "20")
	done <- struct{}{}
}

// one reduce vertex(non keyed)
func (r *ReduceSuite) TestSimpleNonKeyedReducePipeline() {
	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		r.T().SkipNow()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

	defer cancel()
	w := r.Given().Pipeline("@testdata/simple-non-keyed-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "reduce-sum"

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

	// since there is no key, all the messages will be assigned to same window
	// the sum should be 60(since the window is 10s)
	w.Expect().RedisSinkContains("reduce-sum-sink", "60")
	done <- struct{}{}
}

// two reduce vertex(keyed and non keyed)
func (r *ReduceSuite) TestComplexReducePipelineKeyedNonKeyed() {
	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		r.T().SkipNow()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

	defer cancel()
	w := r.Given().Pipeline("@testdata/complex-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "complex-sum"

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
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime))
			}
		}
	}()

	// since the key can be even or odd and the first window duration is 10s(which is keyed)
	// and the second window duration is 60s(non-keyed)
	// the sum should be 180(60 + 120)
	w.Expect().RedisSinkContains("complex-sum-sink", "180")
	done <- struct{}{}
}

func (r *ReduceSuite) TestSimpleReducePipelineFailOverUsingWAL() {
	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		r.T().SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

	defer cancel()
	w := r.Given().Pipeline("@testdata/simple-reduce-pipeline-wal.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "even-odd-sum"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	args := "kubectl delete po -n numaflow-system -l " +
		"numaflow.numaproj.io/pipeline-name=even-odd-sum,numaflow.numaproj.io/vertex-name=compute-sum"

	// Kill the reducer pods before processing to trigger failover.
	w.Exec("/bin/sh", []string{"-c", args}, CheckPodKillSucceeded)
	done := make(chan struct{})

	w.Expect().VertexPodsRunning()

	go func() {
		startTime := int(time.Unix(1000, 0).UnixMilli())
		for i := 1; true; i++ {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			default:
				eventTime := strconv.Itoa(startTime + (i * 1000))
				if i == 5 {
					// Kill the reducer pods during processing to trigger failover.
					w.Expect().VertexPodsRunning()
					w.Exec("/bin/sh", []string{"-c", args}, CheckPodKillSucceeded)
					w.Expect().VertexPodsRunning()
				}
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("3")).WithHeader("X-Numaflow-Event-Time", eventTime))
			}
		}
	}()

	w.Expect().
		RedisSinkContains("even-odd-sum-sink", "38").
		RedisSinkContains("even-odd-sum-sink", "76").
		RedisSinkContains("even-odd-sum-sink", "120").
		RedisSinkContains("even-odd-sum-sink", "240")

	done <- struct{}{}
}

// two reduce vertices (keyed and non-keyed) followed by a sliding window vertex
func (r *ReduceSuite) TestComplexSlidingWindowPipeline() {
	// the reduce feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		r.T().SkipNow()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)

	defer cancel()
	w := r.Given().Pipeline("@testdata/complex-sliding-window-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "complex-sliding-sum"

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
				// send the number "1" and "2" to the pipeline every second
				eventTime := strconv.Itoa(startTime + i*1000)
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime)).
					SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime))
			}
		}
	}()

	// At the keyed reduce vertex, the 5-second fixed window produces output every 5 seconds as
	// {key: "even", value: 10(2*5s=10)} and {key: "odd", value: 5(1*5s=5)}.
	// At the non-keyed reduce vertex, the 10-second fixed window produces output every 10 seconds as
	// {value: 30} ((10+5)*(10s/5s) = 30)
	// At the non-keyed sliding window vertex,
	// the sliding window is configured with length 60s.
	// At the first 10s, the window output is {value: 30}
	// At the second 10s, the output is {value: 60} (30+30 = 60)
	// It goes on like this, and at the 6th 10s, the output is {value: 180}
	// At the 7th 10s, the output remains 180 as the window slides forward.

	// we only have to extend the timeout for the first output to be produced. for the rest,
	// we just need to wait for the default timeout for the rest of the outputs since its synchronous
	w.Expect().
		RedisSinkContains("complex-sliding-sum-sink", "30").
		RedisSinkContains("complex-sliding-sum-sink", "60").
		RedisSinkNotContains("complex-sliding-sum-sink", "80").
		RedisSinkContains("complex-sliding-sum-sink", "90").
		RedisSinkContains("complex-sliding-sum-sink", "180").
		RedisSinkNotContains("complex-sliding-sum-sink", "210")
	done <- struct{}{}
}

func TestReduceSuite(t *testing.T) {
	suite.Run(t, new(ReduceSuite))
}
