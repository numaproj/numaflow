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
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type MapSuite struct {
	E2ESuite
}

func (s *MapSuite) TestBatchMapUDFunctionAndSink() {
	w := s.Given().Pipeline("@testdata/flatmap-batch.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "flatmap-batch"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStartedRustRuntime).
		VertexPodLogContains("go-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("rust-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("rust-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello,hello"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello")))

	w.Expect().
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("rust-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("java-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3))
}

func (s *MapSuite) TestUDFunctionAndSink() {
	w := s.Given().Pipeline("@testdata/flatmap.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "flatmap"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStartedRustRuntime).
		VertexPodLogContains("go-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello,hello"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello")))

	w.Expect().
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("java-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3)).
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(3))
}

func (s *MapSuite) TestMapStreamUDFunctionAndSink() {
	w := s.Given().Pipeline("@testdata/flatmap-stream.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "flatmap-stream"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStartedRustRuntime).
		VertexPodLogContains("go-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("go-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("python-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-split", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("java-udsink", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello,hello,hello"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("hello")))

	w.Expect().
		VertexPodLogContains("go-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
	w.Expect().
		VertexPodLogContains("go-udsink-2", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
	w.Expect().
		VertexPodLogContains("python-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
	w.Expect().
		VertexPodLogContains("java-udsink", "hello", PodLogCheckOptionWithContainer("udsink"), PodLogCheckOptionWithCount(4))
}

func (s *MapSuite) TestPipelineRateLimitWithRedisStore() {
	w := s.Given().Pipeline("@testdata/rate-limit-redis.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "rate-limit-redis"

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStartedRustRuntime).
		VertexPodLogContains("map-udf", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("out", LogSinkVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Wait for messages to start flowing
	time.Sleep(10 * time.Second)

	// port-forward daemon server
	defer w.DaemonPodPortForward(pipelineName, 1234, dfv1.DaemonServicePort).
		TerminateAllPodPortForwards()

	// Verify rate limiting is working by checking processing rates
	client, err := daemonclient.NewGRPCDaemonServiceClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()

	// Check processing rates for the UDF vertex which has rate limiting applied
	timer := time.NewTimer(2 * time.Minute)
	waitInterval := 5 * time.Second
	succeedChan := make(chan struct{})

	go func() {
		const stableDuration = 20 * time.Second

		// First loop: Wait until rate reaches 99-100 TPS range
		reachedStableRange := false
		for !reachedStableRange {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				return
			default:
				m, err := client.GetVertexMetrics(context.Background(), pipelineName, "map-udf")
				if err != nil {
					time.Sleep(waitInterval)
					continue
				}

				if len(m) == 0 {
					time.Sleep(waitInterval)
					continue
				}

				oneMinRate := m[0].ProcessingRates["1m"]
				if oneMinRate == nil {
					time.Sleep(waitInterval)
					continue
				}

				currentRate := oneMinRate.GetValue()
				// The rate limit is set to 100 TPS, so processing rate should be around 100 or less
				// Allow some tolerance for measurement variations (99-100 TPS)
				if currentRate >= 99 && currentRate <= 100 {
					s.T().Logf("Rate reached stable range: %.2f TPS. Starting stability verification...", currentRate)
					reachedStableRange = true
				} else {
					s.T().Logf("Current processing rate: %.2f TPS, waiting for rate to reach 99-100 TPS range...", currentRate)
					time.Sleep(waitInterval)
				}
			}
		}

		// Second loop: Verify rate stays in 99-100 TPS range for 20 seconds
		stableStartTime := time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				return
			default:
				m, err := client.GetVertexMetrics(context.Background(), pipelineName, "map-udf")
				if err != nil {
					break
				}

				oneMinRate := m[0].ProcessingRates["1m"]
				currentRate := oneMinRate.GetValue()
				if currentRate >= 99 && currentRate <= 100 {
					// Check if we've been stable for the required duration
					if time.Since(stableStartTime) >= stableDuration {
						s.T().Logf("Rate limiting working correctly and stable for %v. Processing rate: %.2f TPS (expected: 99-100 TPS)", stableDuration, currentRate)
						succeedChan <- struct{}{}
						return
					}
					s.T().Logf("Rate stable: %.2f TPS, stable for: %v (need %v)", currentRate, time.Since(stableStartTime).Round(time.Second), stableDuration)
				} else {
					// Rate regressed, restart stability verification
					s.T().Logf("Rate regressed from stable range: %.2f TPS. Restarting stability verification...", currentRate)
					stableStartTime = time.Now()
				}
				time.Sleep(waitInterval)
			}
		}
	}()

	select {
	case <-succeedChan:
		// Success - rate limiting is working
	case <-timer.C:
		assert.Fail(s.T(), "timed out waiting for rate limiting to take effect")
	}
	timer.Stop()
}

func TestMapSuite(t *testing.T) {
	suite.Run(t, new(MapSuite))
}
