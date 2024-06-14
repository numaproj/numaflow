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

package http_e2e

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

//go:generate kubectl apply -f testdata/http-auth-fake-secret.yaml -n numaflow-system
type HTTPSuite struct {
	E2ESuite
}

func (s *HTTPSuite) TestHTTPSourcePipeline() {
	w := s.Given().Pipeline("@testdata/http-source.yaml").
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
	w.Expect().SinkContains("out", "no-id", SinkCheckWithContainCount(2))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("with-id")).WithHeader("x-numaflow-id", "101")).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("with-id")).WithHeader("x-numaflow-id", "101"))
	// With same x-numaflow-id, expect 1 output
	w.Expect().SinkContains("out", "with-id", SinkCheckWithContainCount(1))

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("with-id")).WithHeader("x-numaflow-id", "102"))
	// With a new x-numaflow-id, expect 2 outputs
	w.Expect().SinkContains("out", "with-id", SinkCheckWithContainCount(2))
}

func (s *HTTPSuite) TestHTTPSourceAuthPipeline() {
	w := s.Given().Pipeline("@testdata/http-source-with-auth.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "http-auth-source"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("no-auth"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("with-auth")).WithHeader("Authorization", "Bearer faketoken"))
	w.Expect().SinkContains("out", "with-auth")
	w.Expect().SinkNotContains("out", "no-auth")
}

func TestHTTPSuite(t *testing.T) {
	suite.Run(t, new(HTTPSuite))
}
