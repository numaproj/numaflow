//go:build test

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type FunctionalSuite struct {
	E2ESuite
}

func (s *FunctionalSuite) TestCreateSimplePipeline() {
	w := s.Given().Pipeline("@testdata/simple-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "simple-pipeline"

	w.Expect().
		VertexPodsRunning().DaemonPodsRunning().
		VertexPodLogContains("input", LogSourceVertexStarted).
		VertexPodLogContains("p1", LogUDFVertexStarted, PodLogCheckOptionWithContainer("main")).
		VertexPodLogContains("output", LogSinkVertexStarted).
		DaemonPodLogContains(pipelineName, LogDaemonStarted).
		VertexPodLogContains("output", `"Data":".*","Createdts":.*`)

	defer w.VertexPodPortForward("input", 8001, dfv1.VertexMetricsPort).
		VertexPodPortForward("p1", 8002, dfv1.VertexMetricsPort).
		VertexPodPortForward("output", 8003, dfv1.VertexMetricsPort).
		DaemonPodPortForward(pipelineName, 1234, dfv1.DaemonServicePort).
		TerminateAllPodPortForwards()

	// Check vertex pod metrics endpoints
	HTTPExpect(s.T(), "https://localhost:8001").GET("/metrics").
		Expect().
		Status(200).Body().Contains("total")

	HTTPExpect(s.T(), "https://localhost:8002").GET("/metrics").
		Expect().
		Status(200).Body().Contains("total")

	HTTPExpect(s.T(), "https://localhost:8003").GET("/metrics").
		Expect().
		Status(200).Body().Contains("total")

	// Test daemon service with REST
	HTTPExpect(s.T(), "https://localhost:1234").GET(fmt.Sprintf("/api/v1/pipelines/%s/buffers", pipelineName)).
		Expect().
		Status(200).Body().Contains("buffers")

	HTTPExpect(s.T(), "https://localhost:1234").
		GET(fmt.Sprintf("/api/v1/pipelines/%s/buffers/%s", pipelineName, dfv1.GenerateEdgeBufferName(Namespace, pipelineName, "input", "p1"))).
		Expect().
		Status(200).Body().Contains("pipeline")

	HTTPExpect(s.T(), "https://localhost:1234").
		GET(fmt.Sprintf("/api/v1/pipelines/%s/vertices/%s/metrics", pipelineName, "p1")).
		Expect().
		Status(200).Body().Contains("pipeline")

	// Test Daemon service with gRPC
	client, err := daemonclient.NewDaemonServiceClient("localhost:1234")
	assert.NoError(s.T(), err)
	buffers, err := client.ListPipelineBuffers(context.Background(), pipelineName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 2, len(buffers))
	bufferInfo, err := client.GetPipelineBuffer(context.Background(), pipelineName, dfv1.GenerateEdgeBufferName(Namespace, pipelineName, "input", "p1"))
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), "input", *bufferInfo.FromVertex)
	m, err := client.GetVertexMetrics(context.Background(), pipelineName, "p1")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), pipelineName, m.Pipeline)
}

func (s *FunctionalSuite) TestFiltering() {
	w := s.Given().Pipeline("@testdata/filtering.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("p1", LogUDFVertexStarted, PodLogCheckOptionWithContainer("main")).
		VertexPodLogContains("out", LogSinkVertexStarted)

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte(`{"id": 180, "msg": "hello", "expect0": "fail", "desc": "A bad example"}`)).
		Expect().
		Status(204)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte(`{"id": 80, "msg": "hello1", "expect1": "fail", "desc": "A bad example"}`)).
		Expect().
		Status(204)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte(`{"id": 80, "msg": "hello", "expect2": "fail", "desc": "A bad example"}`)).
		Expect().
		Status(204)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte(`{"id": 80, "msg": "hello", "expect3": "succeed", "desc": "A good example"}`)).
		Expect().
		Status(204)

	w.Expect().VertexPodLogContains("out", "expect3")
	w.Expect().VertexPodLogNotContains("out", "expect[0-2]", PodLogCheckOptionWithTimeout(2*time.Second))
}

func (s *FunctionalSuite) TestConditionalForwarding() {
	w := s.Given().Pipeline("@testdata/even-odd.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("even-or-odd", LogUDFVertexStarted, PodLogCheckOptionWithContainer("main")).
		VertexPodLogContains("even-sink", LogSinkVertexStarted).
		VertexPodLogContains("odd-sink", LogSinkVertexStarted).
		VertexPodLogContains("number-sink", LogSinkVertexStarted)
	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("888888")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("888889")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("not an integer")).
		Expect().
		Status(204)

	w.Expect().VertexPodLogContains("even-sink", "888888")
	w.Expect().VertexPodLogNotContains("even-sink", "888889", PodLogCheckOptionWithTimeout(2*time.Second))
	w.Expect().VertexPodLogNotContains("even-sink", "not an integer", PodLogCheckOptionWithTimeout(2*time.Second))

	w.Expect().VertexPodLogContains("odd-sink", "888889")
	w.Expect().VertexPodLogNotContains("odd-sink", "888888", PodLogCheckOptionWithTimeout(2*time.Second))
	w.Expect().VertexPodLogNotContains("odd-sink", "not an integer", PodLogCheckOptionWithTimeout(2*time.Second))

	w.Expect().VertexPodLogContains("number-sink", "888888")
	w.Expect().VertexPodLogContains("number-sink", "888889")
	w.Expect().VertexPodLogNotContains("number-sink", "not an integer", PodLogCheckOptionWithTimeout(2*time.Second))
}

func (s *FunctionalSuite) TestFlatmapUDF() {
	w := s.Given().Pipeline("@testdata/flatmap.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("split", LogUDFVertexStarted, PodLogCheckOptionWithContainer("main")).
		VertexPodLogContains("out", LogSinkVertexStarted)

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("hello,hello")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("hello")).
		Expect().
		Status(204)

	w.Expect().VertexPodLogContains("out", "hello", PodLogCheckOptionWithCount(3))
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
