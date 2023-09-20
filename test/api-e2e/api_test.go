package api_e2e

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type APISuite struct {
	E2ESuite
}

func (s *APISuite) TestGetSysInfo() {
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

func (s *APISuite) TestAPI() {
	var err error
	numaflowServerPodName := s.GetNumaflowServerPodName()
	if numaflowServerPodName == "" {
		panic("failed to find the nuamflow-server pod")
	}
	stopPortForward := s.StartPortForward(numaflowServerPodName, 8443)

	namespaceBody := HTTPExpect(s.T(), "https://localhost:8443").GET("/api/v1_1/namespaces").
		Expect().
		Status(200).Body().Raw()
	var namespaceExpect = `numaflow-system`
	assert.Contains(s.T(), namespaceBody, namespaceExpect)

	var pl1 v1alpha1.Pipeline
	err = json.Unmarshal(testPipeline1, &pl1)
	assert.NoError(s.T(), err)
	createPipeline1 := HTTPExpect(s.T(), "https://localhost:8443").POST(fmt.Sprintf("/api/v1_1/namespaces/%s/pipelines", Namespace)).WithJSON(pl1).
		Expect().
		Status(200).Body().Raw()
	var pl2 v1alpha1.Pipeline
	err = json.Unmarshal(testPipeline2, &pl2)
	assert.NoError(s.T(), err)
	createPipeline2 := HTTPExpect(s.T(), "https://localhost:8443").POST(fmt.Sprintf("/api/v1_1/namespaces/%s/pipelines", Namespace)).WithJSON(pl2).
		Expect().
		Status(200).Body().Raw()
	var createPipelineSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), createPipeline1, createPipelineSuccessExpect)
	assert.Contains(s.T(), createPipeline2, createPipelineSuccessExpect)

	clusterSummaryBody := HTTPExpect(s.T(), "https://localhost:8443").GET("/api/v1_1/cluster-summary").
		Expect().
		Status(200).Body().Raw()
	var clusterSummaryExpect = `{"namespace":"numaflow-system","pipelineSummary":{"active":{"Healthy":2,"Warning":0,"Critical":0},"inactive":0},"isbServiceSummary":{"active":{"Healthy":1,"Warning":0,"Critical":0},"inactive":0}}`
	assert.Contains(s.T(), clusterSummaryBody, clusterSummaryExpect)

	listPipelineBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1_1/namespaces/%s/pipelines", Namespace)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), listPipelineBody, testPipeline1Name)
	assert.Contains(s.T(), listPipelineBody, testPipeline2Name)

	getPipelineBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1_1/namespaces/%s/pipelines/%s", Namespace, testPipeline1Name)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), getPipelineBody, fmt.Sprintf(`"name":"%s"`, testPipeline1Name))
	assert.Contains(s.T(), getPipelineBody, `"status":"healthy"`)

	var testISBSVC v1alpha1.InterStepBufferService
	err = json.Unmarshal(testISBSVCSpec, &testISBSVC)
	assert.NoError(s.T(), err)
	createISBSVCBody := HTTPExpect(s.T(), "https://localhost:8443").POST(fmt.Sprintf("/api/v1_1/namespaces/%s/isb-services", Namespace)).WithJSON(testISBSVC).
		Expect().
		Status(200).Body().Raw()
	var createISBSVCSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), createISBSVCBody, createISBSVCSuccessExpect)

	listISBSVCBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1_1/namespaces/%s/isb-services", Namespace)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), listISBSVCBody, testISBSVCName)

	getISBSVCBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1_1/namespaces/%s/isb-services/%s", Namespace, testISBSVCName)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), getISBSVCBody, fmt.Sprintf(`"name":"%s"`, testISBSVCName))
	assert.Contains(s.T(), getISBSVCBody, `"status":"healthy"`)

	deletePipeline1 := HTTPExpect(s.T(), "https://localhost:8443").DELETE(fmt.Sprintf("/api/v1_1/namespaces/%s/pipelines/%s", Namespace, testPipeline1Name)).
		Expect().
		Status(200).Body().Raw()
	deletePipeline2 := HTTPExpect(s.T(), "https://localhost:8443").DELETE(fmt.Sprintf("/api/v1_1/namespaces/%s/pipelines/%s", Namespace, testPipeline2Name)).
		Expect().
		Status(200).Body().Raw()
	var deletePipelineSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), deletePipeline1, deletePipelineSuccessExpect)
	assert.Contains(s.T(), deletePipeline2, deletePipelineSuccessExpect)

	deleteISBSVC := HTTPExpect(s.T(), "https://localhost:8443").DELETE(fmt.Sprintf("/api/v1_1/namespaces/%s/isb-services/%s", Namespace, testISBSVCName)).
		Expect().
		Status(200).Body().Raw()
	var deleteISBSVCSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), deleteISBSVC, deleteISBSVCSuccessExpect)

	stopPortForward()
}

func TestAPISuite(t *testing.T) {
	suite.Run(t, new(APISuite))
}
