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
	"testing"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type MonoVertexSuite struct {
	E2ESuite
}

func (s *MonoVertexSuite) TestMonoVertexWithTransformer() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-with-transformer.yaml").
		When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().MonoVertexPodsRunning()

	// Expect the messages to be processed by the transformer.
	w.Expect().MonoVertexPodLogContains("AssignEventTime", PodLogCheckOptionWithContainer("transformer"))

	// Expect the messages to reach the sink.
	w.Expect().RedisSinkContains("transformer-mono-vertex", "199")
	w.Expect().RedisSinkContains("transformer-mono-vertex", "200")
}

func TestMonoVertexSuite(t *testing.T) {
	suite.Run(t, new(MonoVertexSuite))
}
