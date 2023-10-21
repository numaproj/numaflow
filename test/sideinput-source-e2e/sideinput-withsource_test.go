package sideinput_source_e2e

import (
	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

type SideInputSource struct {
	E2ESuite
}

func (s *SideInputSource) TestSourceWithSideInput() {
	w := s.Given().Pipeline("@testdata/sideinput_source.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()
	w.Expect().SinkContains("redis-uds", "e2e-even", WithTimeout(5*time.Minute))

}

func TestSideInputSourceSuite(t *testing.T) {
	suite.Run(t, new(SideInputSource))
}
