//go:build test

package http_e2e

import (
	"fmt"
	"testing"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
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

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("out", LogSinkVertexStarted)

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	// Check Service
	cmd := fmt.Sprintf("kubectl -n %s get svc -lnumaflow.numaproj.io/pipeline-name=%s,numaflow.numaproj.io/vertex-name=%s | grep -v CLUSTER-IP | grep -v headless", Namespace, "http-source", "in")
	w.Exec("sh", []string{"-c", cmd}, OutputRegexp("http-source-in"))

	HTTPExpect(s.T(), "https://localhost:8443").GET("/health").Expect().Status(204)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("no-id")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("no-id")).
		Expect().
		Status(204)
	// No x-numaflow-id, expect 2 outputs
	w.Expect().VertexPodLogContains("out", "no-id", PodLogCheckOptionWithCount(2))

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("x-numaflow-id", "101").WithBytes([]byte("with-id")).
		Expect().
		Status(204)
	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("x-numaflow-id", "101").WithBytes([]byte("with-id")).
		Expect().
		Status(204)
	// With same x-numaflow-id, expect 1 output
	w.Expect().VertexPodLogContains("out", "with-id", PodLogCheckOptionWithCount(1))

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("x-numaflow-id", "102").WithBytes([]byte("with-id")).
		Expect().
		Status(204)
	// With a new x-numaflow-id, expect 2 outputs
	w.Expect().VertexPodLogContains("out", "with-id", PodLogCheckOptionWithCount(2))
}

func (s *HTTPSuite) TestHTTPSourceAuthPipeline() {
	w := s.Given().Pipeline("@testdata/http-source-with-auth.yaml").
		When().
		CreatePipelineAndWait()

	defer w.DeletePipelineAndWait()

	w.Expect().
		VertexPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("out", LogSinkVertexStarted)

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithBytes([]byte("no-auth")).
		Expect().
		Status(403)

	HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("Authorization", "Bearer faketoken").WithBytes([]byte("with-auth")).
		Expect().
		Status(204)

	w.Expect().VertexPodLogContains("out", "with-auth")
}

func TestHTTPSuite(t *testing.T) {
	suite.Run(t, new(HTTPSuite))
}
