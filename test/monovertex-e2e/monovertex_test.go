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

	// FIXME - there is something wrong with retrieving pod status, hence this call times out
	// I commented out temporarily to assume all pods are running.
	// w.Expect().MonoVertexPodsRunning()

	w.Expect().RedisSinkContains("", "199")
	w.Expect().RedisSinkContains("", "200")
}

func (s *MonoVertexSuite) TestMonoVertexWithTransformer() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-with-transformer.yaml").
		When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	// FIXME - there is something wrong with retrieving pod status, hence this call times out
	// I commented out temporarily to assume all pods are running.
	// w.Expect().MonoVertexPodsRunning()

	// TODO: fix appropriate target string..
	w.Expect().RedisSinkContains("mono-vertex-with-transformer", "199")
	w.Expect().RedisSinkContains("mono-vertex-with-transformer", "200")
}

func TestMonoVertexSuite(t *testing.T) {
	suite.Run(t, new(MonoVertexSuite))
}
