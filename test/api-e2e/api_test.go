package api_e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

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
		panic("failed to find the numaflow-server pod")
	}
	stopPortForward := s.StartPortForward(numaflowServerPodName, 8443)

	sysinfoBody := HTTPExpect(s.T(), "https://localhost:8443").GET("/api/v1/sysinfo").
		Expect().
		Status(200).Body().Raw()
	var sysinfoExpect = `{"data":{"managedNamespace":"numaflow-system","namespaced":false`
	assert.Contains(s.T(), sysinfoBody, sysinfoExpect)
	assert.Contains(s.T(), sysinfoBody, "Version")
	assert.Contains(s.T(), sysinfoBody, "BuildDate")
	assert.Contains(s.T(), sysinfoBody, "GoVersion")
	assert.Contains(s.T(), sysinfoBody, "Platform")
	stopPortForward()
}

func (s *APISuite) TestISBSVC() {

	var err error
	numaflowServerPodName := s.GetNumaflowServerPodName()
	if numaflowServerPodName == "" {
		panic("failed to find the nuamflow-server pod")
	}
	stopPortForward := s.StartPortForward(numaflowServerPodName, 8443)

	var testISBSVC v1alpha1.InterStepBufferService
	err = json.Unmarshal(testISBSVCSpec, &testISBSVC)
	assert.NoError(s.T(), err)
	createISBSVCBody := HTTPExpect(s.T(), "https://localhost:8443").POST(fmt.Sprintf("/api/v1/namespaces/%s/isb-services", Namespace)).WithJSON(testISBSVC).
		Expect().
		Status(200).Body().Raw()
	var createISBSVCSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), createISBSVCBody, createISBSVCSuccessExpect)

	listISBSVCBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services", Namespace)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), listISBSVCBody, testISBSVCName)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	getISBSVCBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCName)).
		Expect().
		Status(200).Body().Raw()
	for !strings.Contains(getISBSVCBody, `"status":"healthy"`) {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				s.T().Fatalf("failed to get namespaces/isb-services: %v", ctx.Err())
			}
		default:
			time.Sleep(100 * time.Millisecond)
			getISBSVCBody = HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCName)).
				Expect().
				Status(200).Body().Raw()
		}
	}
	assert.Contains(s.T(), getISBSVCBody, fmt.Sprintf(`"name":"%s"`, testISBSVCName))

	deleteISBSVC := HTTPExpect(s.T(), "https://localhost:8443").DELETE(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCName)).
		Expect().
		Status(200).Body().Raw()
	var deleteISBSVCSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), deleteISBSVC, deleteISBSVCSuccessExpect)

	stopPortForward()
}

func (s *APISuite) TestISBSVCReplica1() {

	var err error
	numaflowServerPodName := s.GetNumaflowServerPodName()
	if numaflowServerPodName == "" {
		panic("failed to find the nuamflow-server pod")
	}
	stopPortForward := s.StartPortForward(numaflowServerPodName, 8443)

	var testISBSVC v1alpha1.InterStepBufferService
	err = json.Unmarshal(testISBSVCReplica1Spec, &testISBSVC)
	assert.NoError(s.T(), err)
	createISBSVCBody := HTTPExpect(s.T(), "https://localhost:8443").POST(fmt.Sprintf("/api/v1/namespaces/%s/isb-services", Namespace)).WithJSON(testISBSVC).
		Expect().
		Status(200).Body().Raw()
	var createISBSVCSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), createISBSVCBody, createISBSVCSuccessExpect)

	listISBSVCBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services", Namespace)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), listISBSVCBody, testISBSVCReplica1Name)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	getISBSVCBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCReplica1Name)).
		Expect().
		Status(200).Body().Raw()
	for !strings.Contains(getISBSVCBody, `"status":"healthy"`) {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				s.T().Fatalf("failed to get namespaces/isb-services: %v", ctx.Err())
			}
		default:
			time.Sleep(100 * time.Millisecond)
			getISBSVCBody = HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCReplica1Name)).
				Expect().
				Status(200).Body().Raw()
		}
	}
	assert.Contains(s.T(), getISBSVCBody, fmt.Sprintf(`"name":"%s"`, testISBSVCReplica1Name))

	deleteISBSVC := HTTPExpect(s.T(), "https://localhost:8443").DELETE(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCReplica1Name)).
		Expect().
		Status(200).Body().Raw()
	var deleteISBSVCSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), deleteISBSVC, deleteISBSVCSuccessExpect)

	stopPortForward()
}

func (s *APISuite) TestPipeline0() {
	var err error
	numaflowServerPodName := s.GetNumaflowServerPodName()
	if numaflowServerPodName == "" {
		panic("failed to find the nuamflow-server pod")
	}
	stopPortForward := s.StartPortForward(numaflowServerPodName, 8443)

	namespaceBody := HTTPExpect(s.T(), "https://localhost:8443").GET("/api/v1/namespaces").
		Expect().
		Status(200).Body().Raw()
	var namespaceExpect = `numaflow-system`
	assert.Contains(s.T(), namespaceBody, namespaceExpect)

	var pl1 v1alpha1.Pipeline
	err = json.Unmarshal(testPipeline1, &pl1)
	assert.NoError(s.T(), err)
	createPipeline1 := HTTPExpect(s.T(), "https://localhost:8443").POST(fmt.Sprintf("/api/v1/namespaces/%s/pipelines", Namespace)).WithJSON(pl1).
		Expect().
		Status(200).Body().Raw()
	var pl2 v1alpha1.Pipeline
	err = json.Unmarshal(testPipeline2, &pl2)
	assert.NoError(s.T(), err)
	createPipeline2 := HTTPExpect(s.T(), "https://localhost:8443").POST(fmt.Sprintf("/api/v1/namespaces/%s/pipelines", Namespace)).WithJSON(pl2).
		Expect().
		Status(200).Body().Raw()
	var createPipelineSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), createPipeline1, createPipelineSuccessExpect)
	assert.Contains(s.T(), createPipeline2, createPipelineSuccessExpect)

	var patchPipelineSuccessExpect = `{"data":null}`
	pausePipeline1 := HTTPExpect(s.T(), "https://localhost:8443").PATCH(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, testPipeline1Name)).WithBytes(testPipeline1Pause).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), pausePipeline1, patchPipelineSuccessExpect)

	resumePipeline1 := HTTPExpect(s.T(), "https://localhost:8443").PATCH(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, testPipeline1Name)).WithBytes(testPipeline1Resume).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), resumePipeline1, patchPipelineSuccessExpect)

	clusterSummaryBody := HTTPExpect(s.T(), "https://localhost:8443").GET("/api/v1/cluster-summary").
		Expect().
		Status(200).Body().Raw()
	var clusterSummaryExpect = `{"isEmpty":false,"namespace":"numaflow-system","pipelineSummary":{"active":{"Healthy":2,"Warning":0,"Critical":0},"inactive":0},"isbServiceSummary":{"active":{"Healthy":1,"Warning":0,"Critical":0},"inactive":0}}`
	assert.Contains(s.T(), clusterSummaryBody, clusterSummaryExpect)

	listPipelineBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines", Namespace)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), listPipelineBody, testPipeline1Name)
	assert.Contains(s.T(), listPipelineBody, testPipeline2Name)

	deletePipeline1 := HTTPExpect(s.T(), "https://localhost:8443").DELETE(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, testPipeline1Name)).
		Expect().
		Status(200).Body().Raw()
	deletePipeline2 := HTTPExpect(s.T(), "https://localhost:8443").DELETE(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, testPipeline2Name)).
		Expect().
		Status(200).Body().Raw()
	var deletePipelineSuccessExpect = `{"data":null}`
	assert.Contains(s.T(), deletePipeline1, deletePipelineSuccessExpect)
	assert.Contains(s.T(), deletePipeline2, deletePipelineSuccessExpect)

	stopPortForward()
}

func (s *APISuite) TestPipeline1() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	numaflowServerPodName := s.GetNumaflowServerPodName()
	if numaflowServerPodName == "" {
		panic("failed to find the nuamflow-server pod")
	}
	stopPortForward := s.StartPortForward(numaflowServerPodName, 8443)

	w := s.Given().Pipeline("@testdata/simple-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "simple-pipeline"

	w.Expect().
		VertexPodsRunning().DaemonPodsRunning().
		VertexPodLogContains("input", LogSourceVertexStarted).
		VertexPodLogContains("p1", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("output", SinkVertexStarted).
		DaemonPodLogContains(pipelineName, LogDaemonStarted).
		VertexPodLogContains("output", `"Data":.*,"Createdts":.*`)

	getPipelineISBsBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/isbs", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	for strings.Contains(getPipelineISBsBody, "errMsg") {
		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				s.T().Fatalf("failed to get piplines/isbs: %v", ctx.Err())
			}
		default:
			time.Sleep(100 * time.Millisecond)
			getPipelineISBsBody = HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/isbs", Namespace, pipelineName)).
				Expect().
				Status(200).Body().Raw()
		}
	}

	assert.Contains(s.T(), getPipelineISBsBody, `"bufferName":"numaflow-system-simple-pipeline-p1-0"`)
	assert.Contains(s.T(), getPipelineISBsBody, `"bufferName":"numaflow-system-simple-pipeline-output-0"`)

	getPipelineBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), getPipelineBody, `"name":"simple-pipeline"`)
	assert.Contains(s.T(), getPipelineBody, `"status":"healthy"`)

	getPipelineWatermarksBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/watermarks", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), getPipelineWatermarksBody, `watermarks`)
	assert.Contains(s.T(), getPipelineWatermarksBody, `"edge":"input-p1"`)
	assert.Contains(s.T(), getPipelineWatermarksBody, `"edge":"p1-output"`)

	getVerticesMetricsBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/metrics", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), getVerticesMetricsBody, `"vertex":"input","processingRates"`)
	assert.Contains(s.T(), getVerticesMetricsBody, `"vertex":"p1","processingRates"`)
	assert.Contains(s.T(), getVerticesMetricsBody, `"vertex":"output","processingRates"`)

	getVerticesPodsBody := HTTPExpect(s.T(), "https://localhost:8443").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/input/pods", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), getVerticesPodsBody, `simple-pipeline-input-0`)

	stopPortForward()
}

func TestAPISuite(t *testing.T) {
	suite.Run(t, new(APISuite))
}
