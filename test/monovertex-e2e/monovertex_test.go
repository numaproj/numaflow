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
	firstRetryLog := fmt.Sprintf(`"retry_attempt":"%d"`, 1)
	secondRetryLog := fmt.Sprintf(`"retry_attempt":"%d"`, 2)
	thirdLog := fmt.Sprintf(`"retry_attempt":"%d"`, 3)
	dropLog := "Retries exhausted, dropping messages."
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

	// port-forward mvtx daemon server
	defer w.MvtxDaemonPodPortForward(1234, dfv1.MonoVertexDaemonServicePort).
		TerminateAllPodPortForwards()

	// Verify rate limiting is working by checking processing rates
	client, err := mvtxclient.NewGRPCClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()

	w.Expect().MonoVertexPodLogContains("\"processed\":\"50", PodLogCheckOptionWithContainer("numa"), PodLogCheckOptionWithCount(20))
}

func (s *MonoVertexSuite) TestMonoVertexUserMetadataPropagation() {
	w := s.Given().MonoVertex("@testdata/metadata-monovertex.yaml").
		When().
		CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().MonoVertexPodsRunning()

	w.Expect().
		MonoVertexPodLogContains("Groups at mapper:", PodLogCheckOptionWithContainer("udf")).
		MonoVertexPodLogContains("event-time-group", PodLogCheckOptionWithContainer("udf")).
		MonoVertexPodLogContains("simple-source", PodLogCheckOptionWithContainer("udf"))

	w.Expect().
		MonoVertexPodLogContains("User Metadata:", PodLogCheckOptionWithContainer("udsink")).
		MonoVertexPodLogContains("event-time-group", PodLogCheckOptionWithContainer("udsink")).
		MonoVertexPodLogContains("simple-source", PodLogCheckOptionWithContainer("udsink")).
		MonoVertexPodLogContains("map-group", PodLogCheckOptionWithContainer("udsink")).
		MonoVertexPodLogContains("txn-id", PodLogCheckOptionWithContainer("udsink"))
}

func TestMonoVertexSuite(t *testing.T) {
	suite.Run(t, new(MonoVertexSuite))
}
