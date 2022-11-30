package reducer_e2e

import (
	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"strconv"
	"sync"
	"testing"
	"time"
)

type ReducerSuite struct {
	E2ESuite
}

func (s *ReducerSuite) TestSimpleReducerPipelineFailOver() {
	w := s.Given().Pipeline("@testdata/simple-reduce-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "even-odd-sum"

	w.Expect().
		VertexPodsRunning().DaemonPodsRunning().
		VertexPodLogContains("in", LogSourceVertexStarted).
		VertexPodLogContains("atoi", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("sink", LogSinkVertexStarted).
		DaemonPodLogContains(pipelineName, LogDaemonStarted)

	defer w.VertexPodPortForward("in", 8443, dfv1.VertexHTTPSPort).
		TerminateAllPodPortForwards()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startTime := int(time.Unix(1000, 0).UnixMilli())
		for i := 1; i <= 100; i++ {
			for j := 0; j < 1; j++ {
				eventTime := startTime + (i * 1000)
				HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("X-Numaflow-Event-Time", strconv.Itoa(eventTime)).WithBytes([]byte("1")).
					Expect().
					Status(204)

				HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("X-Numaflow-Event-Time", strconv.Itoa(eventTime)).WithBytes([]byte("2")).
					Expect().
					Status(204)

				HTTPExpect(s.T(), "https://localhost:8443").POST("/vertices/in").WithHeader("X-Numaflow-Event-Time", strconv.Itoa(eventTime)).WithBytes([]byte("3")).
					Expect().
					Status(204)
			}
		}

		w.Expect().VertexPodLogContains("sink", "Payload -  120")
		w.Expect().VertexPodLogContains("sink", "Payload -  240")
	}()

	// Kill one of the pod during processing to trigger failover.
	block := func(t *testing.T, output string, err error) {
		assert.Contains(t, output, "deleted")
		assert.NoError(t, err)
	}
	args := "kubectl delete po `kubectl -n numaflow-system get po " +
		"-lnumaflow.numaproj.io/pipeline-name=even-odd-sum,numaflow.numaproj.io/vertex-name=compute-sum | " +
		"grep -v NAME | head -1 | awk '{print $1}'`"
	w.Exec("/bin/sh", []string{"-c", args}, block)

	wg.Wait()
}

func TestReducerSuite(t *testing.T) {
	suite.Run(t, new(ReducerSuite))
}
