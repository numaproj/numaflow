package monovertex_e2e

import (
	"testing"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type MonoVertexSuite struct {
	E2ESuite
}

func (s *MonoVertexSuite) TestSimpleMonoVertex() {
	w := s.Given().MonoVertex("@testdata/simple-mono-vertex.yaml").
		When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().MonoVertexPodsRunning()

	w.Expect().RedisSinkContains("simple-mono-vertex", "199")
	w.Expect().RedisSinkContains("simple-mono-vertex", "200")
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
