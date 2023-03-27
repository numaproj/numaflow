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

package redis_source_e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/numaproj/numaflow/test/fixtures"
)

//go:generate kubectl -n numaflow-system delete statefulset nats --ignore-not-found=true
//go:generate kubectl apply -k ../../config/apps/redis -n numaflow-system
type RedisSourceSuite struct {
	fixtures.E2ESuite
}

func (rss *RedisSourceSuite) TestRedisSource() {

	time.Sleep(10 * time.Second) //todo: replace this with waiting for "redis" service to be available

	// can do 2 tests
	// first one is to start from the beginning of the Stream
	// second is to start from the most recent messages of the Stream
	for _, tt := range []struct {
		stream          string
		manifest        string
		expectedNumMsgs int // expected result
	}{
		{"test-stream-a", "@testdata/redis-source-pipeline-from-beginning.yaml", 102},
		{"test-stream-b", "@testdata/redis-source-pipeline-from-end.yaml", 100},
	} {
		fixtures.PumpRedisStream(tt.stream, 2, 20*time.Millisecond, 10, "test-message")

		w := rss.Given().Pipeline(tt.manifest).
			When().
			CreatePipelineAndWait()

		// wait for all the pods to come up
		w.Expect().VertexPodsRunning()

		fixtures.PumpRedisStream(tt.stream, 100, 20*time.Millisecond, 10, "test-message")
		w.Expect().SinkContains("out", "test-message", fixtures.WithContainCount(tt.expectedNumMsgs))

		w.DeletePipelineAndWait()
	}

	time.Sleep(2 * time.Minute) // todo: delete
}
func TestRedisSourceSuite(t *testing.T) {
	suite.Run(t, new(RedisSourceSuite))
}
