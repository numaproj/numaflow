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
	"encoding/json"
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

	// need to wait for Redis to be fully up and running
	// tried to wait for Pod to be in Running phase, but that doesn't seem to be sufficient for it being ready
	// so for now just using a timer
	time.Sleep(10 * time.Second)

	keysValues := map[string]string{"test-msg-1": "test-val-1", "test-msg-2": "test-val-2"}
	keysValuesJson, err := json.Marshal(keysValues)
	if err != nil {
		rss.Fail(err.Error())
	}

	// can do 2 tests
	// 1. start from the beginning of the Stream
	// 2. start from the most recent messages of the Stream
	for _, tt := range []struct {
		stream          string
		manifest        string
		expectedNumMsgs int // expected result
	}{
		{"test-stream-a", "@testdata/redis-source-pipeline-from-beginning.yaml", 101},
		{"test-stream-b", "@testdata/redis-source-pipeline-from-end.yaml", 100},
	} {
		// send some messages before creating the Pipeline so we can test both "ReadFromBeginning" and "ReadFromLatest"
		fixtures.PumpRedisStream(tt.stream, 1, 20*time.Millisecond, 10, string(keysValuesJson))

		w := rss.Given().Pipeline(tt.manifest).
			When().
			CreatePipelineAndWait()

		// wait for all the pods to come up
		w.Expect().VertexPodsRunning()

		fixtures.PumpRedisStream(tt.stream, 100, 20*time.Millisecond, 10, string(keysValuesJson))
		time.Sleep(20 * time.Second)
		w.Expect().SinkContains("out", "test-val-1", fixtures.WithContainCount(tt.expectedNumMsgs))
		w.Expect().SinkContains("out", "test-val-2", fixtures.WithContainCount(tt.expectedNumMsgs))

		w.DeletePipelineAndWait()
	}

}
func TestRedisSourceSuite(t *testing.T) {
	suite.Run(t, new(RedisSourceSuite))
}
