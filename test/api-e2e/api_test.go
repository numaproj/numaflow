package api_e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type APISuite struct {
	E2ESuite
}

func (s *APISuite) TestAPI() {
	numaflowServerPodName := s.GetNumaflowServerPodName()
	if numaflowServerPodName == "" {
		panic("failed to find the nuamflow-server pod")
	}
	stopPortForward := s.StartPortForward(numaflowServerPodName, 8443)

	sysinfoBody := HTTPExpect(s.T(), "https://localhost:8443").GET("/api/v1_1/sysinfo").
		Expect().
		Status(200).Body().Raw()
	var sysinfoExpect = `{"data":{"managedNamespace":"numaflow-system","namespaced":false,"version":""}}`
	assert.Contains(s.T(), sysinfoBody, sysinfoExpect)
	stopPortForward()
}

func TestAPISuite(t *testing.T) {
	suite.Run(t, new(APISuite))
}
