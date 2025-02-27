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

package pulsar_e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/numaproj/numaflow/test/fixtures"
)

//go:generate kubectl delete -f testdata/pulsar-minimal.yaml -n numaflow-system --ignore-not-found=true
//go:generate kubectl delete -f testdata/mono-vertex-with-pulsar-source.yaml -n numaflow-system --ignore-not-found=true
//go:generate kubectl apply -f testdata/pulsar-minimal.yaml -n numaflow-system
// Wait for Pulsar to come up
//go:generate kubectl -n numaflow-system wait --for=condition=ready pod -l app=pulsar-broker --timeout 60s

type PulsarSuite struct {
	fixtures.E2ESuite
}

func (ks *PulsarSuite) TestPulsarSource() {
	w := ks.Given().MonoVertex("@testdata/mono-vertex-with-pulsar-source.yaml").
		When().
		CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().MonoVertexPodsRunning()

	fixtures.PumpPulsarTopic("e2e_topic", 200, 10*time.Millisecond, "pulsar")

	// Expect the messages to reach the sink.
	w.Expect().RedisSinkContains("mono-vertex-with-pulsar-source", "pulsar-198")
	w.Expect().RedisSinkContains("mono-vertex-with-pulsar-source", "pulsar-199")
}

func TestPulsarSuite(t *testing.T) {
	suite.Run(t, new(PulsarSuite))
}
