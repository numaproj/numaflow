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

	// TODO: fix redis sink to be able to check for mono vertex output
	// https://github.com/numaproj/numaflow-go/blob/main/pkg/sinker/examples/redis-sink/main.go ( we should use the NUMAFLOW_MONO_VERTEX_NAME env)
	// also redis sink checks only takes pipeline and vertex name, we should support monovertex as well
	w.Expect().RedisSinkContains("simple-mono-vertex", "199")
	w.Expect().RedisSinkContains("simple-mono-vertex", "200")
}

func (s *MonoVertexSuite) TestMonoVertexWithTransformer() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-with-transformer.yaml").
		When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().MonoVertexPodsRunning()

	// TODO: fix appropriate target string..
	w.Expect().RedisSinkContains("mono-vertex-with-transformer", "199")
	w.Expect().RedisSinkContains("mono-vertex-with-transformer", "200")
}

func TestMonoVertexSuite(t *testing.T) {
	suite.Run(t, new(MonoVertexSuite))
}
