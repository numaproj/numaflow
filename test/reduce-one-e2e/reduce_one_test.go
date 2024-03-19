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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type ReduceOneSuite struct {
	E2ESuite
}

func (r *ReduceOneSuite) TestSimpleSessionKeyedPipelineAJava() {
	r.testSimpleSessionKeyedPipeline("java")
}

func (r *ReduceOneSuite) testSimpleSessionKeyedPipeline(lang string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	w := r.Given().Pipeline(fmt.Sprintf("@testdata/simple-session-keyed-counter-pipeline-%s.yaml", lang)).
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := fmt.Sprintf("simple-session-counter-%s", lang)

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
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")).WithHeader("X-Numaflow-Event-Time", eventTime))
				w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")).WithHeader("X-Numaflow-Event-Time", eventTime))
				count += 2
			}
		}
	}()

	w.Expect().SinkContains("sink", "5")
	w.Expect().SinkNotContains("sink", "4", WithTimeout(20*time.Second))
	w.Expect().SinkNotContains("sink", "3", WithTimeout(20*time.Second))
	w.Expect().SinkNotContains("sink", "2", WithTimeout(20*time.Second))
	w.Expect().SinkNotContains("sink", "1", WithTimeout(20*time.Second))
	done <- struct{}{}
}

func TestReduceSuite(t *testing.T) {
	suite.Run(t, new(ReduceOneSuite))
}
