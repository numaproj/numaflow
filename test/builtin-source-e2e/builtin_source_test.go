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

package builtin_source_e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

//go:generate kubectl -n numaflow-system delete statefulset nats --ignore-not-found=true
//go:generate kubectl apply -f testdata/http-auth-fake-secret.yaml -n numaflow-system
//go:generate kubectl apply -k ../../config/apps/nats -n numaflow-system
type BuiltinSourceSuite struct {
	E2ESuite
}

func (bss *BuiltinSourceSuite) TestNatsSource() {
	subject := "test-subject"
	w := bss.Given().Pipeline("@testdata/nats-source-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	PumpNatsSubject(subject, 100, 20*time.Millisecond, 10, "test-message")
	w.Expect().RedisSinkContains("nats-source-e2e-out", "test-message", SinkCheckWithContainCount(100))
}

func (bss *BuiltinSourceSuite) TestHTTPSourcePipeline() {
	w := bss.Given().Pipeline("@testdata/http-source.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "http-source"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	// Check Service
	cmd := fmt.Sprintf("kubectl -n %s get svc -lnumaflow.numaproj.io/pipeline-name=%s,numaflow.numaproj.io/vertex-name=%s | grep -v CLUSTER-IP | grep -v headless", Namespace, "http-source", "in")
	w.Exec("sh", []string{"-c", cmd}, OutputRegexp("http-source-in"))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("no-id"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("no-id")))
	// No x-numaflow-id, expect 2 outputs
	w.Expect().RedisSinkContains("http-source-out", "no-id", SinkCheckWithContainCount(2))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("with-id")).WithHeader("x-numaflow-id", "101")).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("with-id")).WithHeader("x-numaflow-id", "101"))
	// With same x-numaflow-id, expect 1 output
	w.Expect().RedisSinkContains("http-source-out", "with-id", SinkCheckWithContainCount(1))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("with-id")).WithHeader("x-numaflow-id", "102"))
	// With a new x-numaflow-id, expect 2 outputs
	w.Expect().RedisSinkContains("http-source-out", "with-id", SinkCheckWithContainCount(2))
}

func (bss *BuiltinSourceSuite) TestHTTPSourceAuthPipeline() {
	w := bss.Given().Pipeline("@testdata/http-source-with-auth.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "http-auth-source"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("no-auth"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("with-auth")).WithHeader("Authorization", "Bearer faketoken"))
	w.Expect().RedisSinkContains("http-auth-source-out", "with-auth")
	w.Expect().RedisSinkNotContains("http-auth-source-out", "no-auth")
}

func (bss *BuiltinSourceSuite) TestJetstreamSource() {
	const streamName = "test-stream"
	const msgPayload = "jetstream-test-message"
	const msgCount = 100

	// The source pods expect stream to exist
	PumpJetstream(streamName, msgPayload, msgCount)

	w := bss.Given().Pipeline("@testdata/jetstream-source-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	w.Expect().RedisSinkContains("jetstream-source-e2e-out", msgPayload, SinkCheckWithContainCount(msgCount))
}

func TestBuiltinSourceSuite(t *testing.T) {
	suite.Run(t, new(BuiltinSourceSuite))
}
