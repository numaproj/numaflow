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

package monovertex_e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	mvtxclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type MonoVertexSuite struct {
	E2ESuite
}

func (s *MonoVertexSuite) TestMonoVertexWithAllContainers() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-with-all-containers.yaml").
		When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().MonoVertexPodsRunning().MvtxDaemonPodsRunning()
	
	defer w.MonoVertexPodPortForward(8931, dfv1.MonoVertexMetricsPort).
		MvtxDaemonPodPortForward(3232, dfv1.MonoVertexDaemonServicePort).
		TerminateAllPodPortForwards()

	// Check metrics endpoints
	HTTPExpect(s.T(), "https://localhost:8931").GET("/metrics").
		Expect().
		Status(200)

	HTTPExpect(s.T(), "https://localhost:3232").GET("/metrics").
		Expect().
		Status(200)

	// Expect the messages to be processed by the transformer.
	w.Expect().MonoVertexPodLogContains("AssignEventTime", PodLogCheckOptionWithContainer("transformer"))

	// Simulate primary sink failure and check fallback sink
	w.Expect().MonoVertexPodLogContains("Primary sink under maintenance", PodLogCheckOptionWithContainer("udsink"))

	// Expect the messages to reach the fallback sink.
	w.Expect().RedisSinkContains("fallback-sink-key", "1000")
	w.Expect().RedisSinkContains("fallback-sink-key", "1001")

}

func (s *MonoVertexSuite) TestExponentialBackoffRetryStrategy() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-exponential-retry-strategy.yaml").When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()
	w.Expect().MonoVertexPodsRunning()
	firstRetryLog := fmt.Sprintf("retry_attempt=%d", 1)
	secondRetryLog := fmt.Sprintf("retry_attempt=%d", 2)
	thirdLog := fmt.Sprintf("retry_attempt=%d", 3)
	dropLog := "Dropping messages"
	w.Expect().MonoVertexPodLogContains(firstRetryLog, PodLogCheckOptionWithContainer("numa"))
	w.Expect().MonoVertexPodLogContains(secondRetryLog, PodLogCheckOptionWithContainer("numa"))
	w.Expect().MonoVertexPodLogContains(dropLog, PodLogCheckOptionWithContainer("numa"))
	w.Expect().MonoVertexPodLogNotContains(thirdLog, PodLogCheckOptionWithContainer("numa"), PodLogCheckOptionWithTimeout(time.Second))
}

func (s *MonoVertexSuite) TestMonoVertexRateLimitWithRedisStore() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-rate-limit-redis.yaml").
		When().
		CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().
		MonoVertexPodsRunning().
		MvtxDaemonPodsRunning()

	defer w.StreamMonoVertexPodLogs("numa").TerminateAllPodLogs()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Wait for messages to start flowing
	time.Sleep(10 * time.Second)

	// port-forward mvtx daemon server
	defer w.MvtxDaemonPodPortForward(1234, dfv1.MonoVertexDaemonServicePort).
		TerminateAllPodPortForwards()

	// Verify rate limiting is working by checking processing rates
	client, err := mvtxclient.NewGRPCClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()

	// Check processing rates for the MonoVertex which has rate limiting applied
	timer := time.NewTimer(5 * time.Minute)
	waitInterval := 10 * time.Second
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
				m, err := client.GetMonoVertexMetrics(context.Background())
				if err != nil {
					time.Sleep(waitInterval)
					continue
				}

				if m == nil {
					time.Sleep(waitInterval)
					continue
				}

				oneMinRate := m.ProcessingRates["1m"]
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
				m, err := client.GetMonoVertexMetrics(context.Background())
				if err != nil {
					break
				}

				oneMinRate := m.ProcessingRates["1m"]
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

func TestMonoVertexSuite(t *testing.T) {
	suite.Run(t, new(MonoVertexSuite))
}
