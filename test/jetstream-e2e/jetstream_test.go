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

package jetstream_e2e

import (
	"testing"

	"github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

//go:generate kubectl -n numaflow-system delete statefulset nats --ignore-not-found=true
//go:generate kubectl apply -k ../../config/apps/nats -n numaflow-system

type JetstreamSuite struct {
	fixtures.E2ESuite
}

func (ns *JetstreamSuite) TestJetstreamSource() {
	const streamName = "test-stream"
	const msgPayload = "jetstream-test-message"

	// The source pods expects stream to exist
	fixtures.PumpJetstream(streamName, msgPayload, 100)

	w := ns.Given().Pipeline("@testdata/jetstream-source-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	w.Expect().SinkContains("out", msgPayload, fixtures.SinkCheckWithContainCount(100))
}

func TestJetstreamSuite(t *testing.T) {
	suite.Run(t, new(JetstreamSuite))
}
