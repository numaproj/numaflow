//go:build test

package e2e

import (
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

const (
	LogSourceVertexStarted = "Start processing source messages"
	LogSinkVertexStarted   = "Start processing sink messages"
	LogUDFVertexStarted    = "Start processing udf messages"
)

type FunctionalSuite struct {
	E2ESuite
}

func (s *FunctionalSuite) TestCreateSimplePipeline() {
	w := s.Given().Pipeline("@testdata/simple-pipeline.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("input", "main", LogSourceVertexStarted).
		VertexPodLogContains("p1", "main", LogUDFVertexStarted).
		VertexPodLogContains("output", "main", LogSinkVertexStarted).
		VertexPodLogContains("output", "main", `"Data":".*","Createdts":.*`)

	defer w.VertexPodPortForward("input", 8001, dfv1.VertexMetricsPort).
		VertexPodPortForward("p1", 8002, dfv1.VertexMetricsPort).
		VertexPodPortForward("output", 8003, dfv1.VertexMetricsPort).
		TerminateAllPodPortForwards()

	HTTPExpect(s.T(), "https://localhost:8001").GET("/metrics").
		Expect().
		Status(200).
		Body().
		Contains("total")

	HTTPExpect(s.T(), "https://localhost:8002").GET("/metrics").
		Expect().
		Status(200).
		Body().
		Contains("total")

	HTTPExpect(s.T(), "https://localhost:8003").GET("/metrics").
		Expect().
		Status(200).
		Body().
		Contains("total")
}

func (s *FunctionalSuite) TestHTTPPipeline() {
	w := s.Given().Pipeline("@testdata/http-source.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", "main", LogSourceVertexStarted).
		VertexPodLogContains("p1", "main", LogUDFVertexStarted).
		VertexPodLogContains("out", "main", LogSinkVertexStarted).
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
