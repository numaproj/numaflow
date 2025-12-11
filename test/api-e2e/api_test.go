/*
Copyright 2022 The Numaproj Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api_e2e

import (
	"context"
	"encoding/json"
	"errors"
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

func TestAPISuite(t *testing.T) {
	suite.Run(t, new(APISuite))
}

func (s *APISuite) TestGetSysInfo() {
	defer s.Given().When().UXServerPodPortForward(8043, 8443).TerminateAllPodPortForwards()

	sysinfoBody := HTTPExpect(s.T(), "https://localhost:8043").GET("/api/v1/sysinfo").
		Expect().
		Status(200).Body().Raw()
	AssertJsonEqual(s.T(), sysinfoBody, "data.managedNamespace", "numaflow-system")
	AssertJsonExists(s.T(), sysinfoBody, "data.isReadOnly")
	AssertJsonStringContains(s.T(), sysinfoBody, "data.version", "Version")
	AssertJsonStringContains(s.T(), sysinfoBody, "data.version", "BuildDate")
	AssertJsonStringContains(s.T(), sysinfoBody, "data.version", "GoVersion")
	AssertJsonStringContains(s.T(), sysinfoBody, "data.version", "Platform")
}

func (s *APISuite) TestISBSVC() {
	defer s.Given().When().UXServerPodPortForward(8143, 8443).TerminateAllPodPortForwards()

	var testISBSVC v1alpha1.InterStepBufferService
	err := json.Unmarshal(testISBSVCSpec, &testISBSVC)
	assert.NoError(s.T(), err)
	createISBSVCBody := HTTPExpect(s.T(), "https://localhost:8143").POST(fmt.Sprintf("/api/v1/namespaces/%s/isb-services", Namespace)).WithJSON(testISBSVC).
		Expect().
		Status(200).Body().Raw()
	var createISBSVCSuccessExpect = `"data":null`
	assert.Contains(s.T(), createISBSVCBody, createISBSVCSuccessExpect)

	listISBSVCBody := HTTPExpect(s.T(), "https://localhost:8143").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services", Namespace)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), listISBSVCBody, testISBSVCName)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	getISBSVCBody := HTTPExpect(s.T(), "https://localhost:8143").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCName)).
		Expect().
		Status(200).Body().Raw()
	for !strings.Contains(getISBSVCBody, `"status":"healthy"`) {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				s.T().Fatalf("failed to get namespaces/isb-services: %v", ctx.Err())
			}
		default:
			time.Sleep(100 * time.Millisecond)
			getISBSVCBody = HTTPExpect(s.T(), "https://localhost:8143").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCName)).
				Expect().
				Status(200).Body().Raw()
		}
	}
	assert.Contains(s.T(), getISBSVCBody, fmt.Sprintf(`"name":"%s"`, testISBSVCName))

	deleteISBSVC := HTTPExpect(s.T(), "https://localhost:8143").DELETE(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCName)).
		Expect().
		Status(200).Body().Raw()
	var deleteISBSVCSuccessExpect = `"data":null`
	assert.Contains(s.T(), deleteISBSVC, deleteISBSVCSuccessExpect)
}

func (s *APISuite) TestISBSVCReplica1() {
	defer s.Given().When().UXServerPodPortForward(8144, 8443).TerminateAllPodPortForwards()

	var testISBSVC v1alpha1.InterStepBufferService
	err := json.Unmarshal(testISBSVCReplica1Spec, &testISBSVC)
	assert.NoError(s.T(), err)
	createISBSVCBody := HTTPExpect(s.T(), "https://localhost:8144").POST(fmt.Sprintf("/api/v1/namespaces/%s/isb-services", Namespace)).WithJSON(testISBSVC).
		Expect().
		Status(200).Body().Raw()
	var createISBSVCSuccessExpect = `"data":null`
	assert.Contains(s.T(), createISBSVCBody, createISBSVCSuccessExpect)

	listISBSVCBody := HTTPExpect(s.T(), "https://localhost:8144").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services", Namespace)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), listISBSVCBody, testISBSVCReplica1Name)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	getISBSVCBody := HTTPExpect(s.T(), "https://localhost:8144").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCReplica1Name)).
		Expect().
		Status(200).Body().Raw()
	for !strings.Contains(getISBSVCBody, `"status":"healthy"`) {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				s.T().Fatalf("failed to get namespaces/isb-services: %v", ctx.Err())
			}
		default:
			time.Sleep(100 * time.Millisecond)
			getISBSVCBody = HTTPExpect(s.T(), "https://localhost:8144").GET(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCReplica1Name)).
				Expect().
				Status(200).Body().Raw()
		}
	}
	assert.Contains(s.T(), getISBSVCBody, fmt.Sprintf(`"name":"%s"`, testISBSVCReplica1Name))

	deleteISBSVC := HTTPExpect(s.T(), "https://localhost:8144").DELETE(fmt.Sprintf("/api/v1/namespaces/%s/isb-services/%s", Namespace, testISBSVCReplica1Name)).
		Expect().
		Status(200).Body().Raw()
	var deleteISBSVCSuccessExpect = `"data":null`
	assert.Contains(s.T(), deleteISBSVC, deleteISBSVCSuccessExpect)
}

func (s *APISuite) TestAPIsForIsbAndPipelineAndMonoVertex() {
	defer s.Given().When().UXServerPodPortForward(8145, 8443).TerminateAllPodPortForwards()

	namespaceBody := HTTPExpect(s.T(), "https://localhost:8145").GET("/api/v1/namespaces").
		Expect().
		Status(200).Body().Raw()
	var namespaceExpect = `numaflow-system`
	assert.Contains(s.T(), namespaceBody, namespaceExpect)

	var pl1 v1alpha1.Pipeline
	err := json.Unmarshal(testPipeline1, &pl1)
	assert.NoError(s.T(), err)
	createPipeline1 := HTTPExpect(s.T(), "https://localhost:8145").POST(fmt.Sprintf("/api/v1/namespaces/%s/pipelines", Namespace)).WithJSON(pl1).
		Expect().
		Status(200).Body().Raw()
	var pl2 v1alpha1.Pipeline
	err = json.Unmarshal(testPipeline2, &pl2)
	assert.NoError(s.T(), err)
	createPipeline2 := HTTPExpect(s.T(), "https://localhost:8145").POST(fmt.Sprintf("/api/v1/namespaces/%s/pipelines", Namespace)).WithJSON(pl2).
		Expect().
		Status(200).Body().Raw()
	var createPipelineSuccessExpect = `"data":null`
	assert.Contains(s.T(), createPipeline1, createPipelineSuccessExpect)
	assert.Contains(s.T(), createPipeline2, createPipelineSuccessExpect)

	var patchPipelineSuccessExpect = `"data":null`
	pausePipeline1 := HTTPExpect(s.T(), "https://localhost:8145").PATCH(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, testPipeline1Name)).WithBytes(testPipeline1Pause).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), pausePipeline1, patchPipelineSuccessExpect)

	resumePipeline1 := HTTPExpect(s.T(), "https://localhost:8145").PATCH(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, testPipeline1Name)).WithBytes(testPipeline1Resume).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), resumePipeline1, patchPipelineSuccessExpect)

	// create a mono vertex
	var mv1 v1alpha1.MonoVertex
	err = json.Unmarshal(testMonoVertex1, &mv1)
	assert.NoError(s.T(), err)
	createMonoVertex := HTTPExpect(s.T(), "https://localhost:8145").POST(fmt.Sprintf("/api/v1/namespaces/%s/mono-vertices", Namespace)).WithJSON(mv1).
		Expect().
		Status(200).Body().Raw()
	var createMonoVertexSuccessExpect = `"data":null`
	assert.Contains(s.T(), createMonoVertex, createMonoVertexSuccessExpect)

	clusterSummaryBody := HTTPExpect(s.T(), "https://localhost:8145").GET("/api/v1/cluster-summary").
		Expect().
		Status(200).Body().Raw()
	AssertJsonEqual(s.T(), clusterSummaryBody, `data.#(namespace=="numaflow-system").isEmpty`, false)
	AssertJsonEqual(s.T(), clusterSummaryBody, `data.#(namespace=="default").isEmpty`, true)
	AssertJsonEqual(s.T(), clusterSummaryBody, `data.#(namespace=="numaflow-system").pipelineSummary`, Json(`{"active":{"Healthy":2,"Warning":0,"Critical":0},"inactive":0}`))
	AssertJsonEqual(s.T(), clusterSummaryBody, `data.#(namespace=="numaflow-system").isbServiceSummary`, Json(`{"active":{"Healthy":1,"Warning":0,"Critical":0},"inactive":0}`))
	AssertJsonEqual(s.T(), clusterSummaryBody, `data.#(namespace=="numaflow-system").monoVertexSummary`, Json(`{"active":{"Healthy":1,"Warning":0,"Critical":0},"inactive":0}`))

	listPipelineBody := HTTPExpect(s.T(), "https://localhost:8145").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines", Namespace)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), listPipelineBody, testPipeline1Name)
	assert.Contains(s.T(), listPipelineBody, testPipeline2Name)

	deletePipeline1 := HTTPExpect(s.T(), "https://localhost:8145").DELETE(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, testPipeline1Name)).
		Expect().
		Status(200).Body().Raw()
	deletePipeline2 := HTTPExpect(s.T(), "https://localhost:8145").DELETE(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, testPipeline2Name)).
		Expect().
		Status(200).Body().Raw()
	var deletePipelineSuccessExpect = `"data":null`
	assert.Contains(s.T(), deletePipeline1, deletePipelineSuccessExpect)
	assert.Contains(s.T(), deletePipeline2, deletePipelineSuccessExpect)

	listMonoVertexBody := HTTPExpect(s.T(), "https://localhost:8145").GET(fmt.Sprintf("/api/v1/namespaces/%s/mono-vertices", Namespace)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), listMonoVertexBody, testMonoVertex1Name)

	// deletes a mono-vertex
	deleteMonoVertex := HTTPExpect(s.T(), "https://localhost:8145").DELETE(fmt.Sprintf("/api/v1/namespaces/%s/mono-vertices/%s", Namespace, testMonoVertex1Name)).
		Expect().
		Status(200).Body().Raw()
	var deleteMonoVertexSuccessExpect = `"data":null`
	assert.Contains(s.T(), deleteMonoVertex, deleteMonoVertexSuccessExpect)

}

func (s *APISuite) TestAPIsForMetricsAndWatermarkAndPodsForPipeline() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	w := s.Given().Pipeline("@testdata/simple-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "simple-pipeline"

	w.Expect().
		VertexPodsRunning().DaemonPodsRunning().
		VertexPodLogContains("input", LogSourceVertexStartedRustRuntime).
		VertexPodLogContains("p1", LogMapVertexStartedRustRuntime, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("output", LogSinkVertexStartedRustRuntime).
		DaemonPodLogContains(pipelineName, LogDaemonStarted).
		VertexPodLogContains("output", `\\"value\\":.*EventTime - \d+`)

	defer w.UXServerPodPortForward(8146, 8443).TerminateAllPodPortForwards()

	getPipelineISBsBody := HTTPExpect(s.T(), "https://localhost:8146").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/isbs", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	for !strings.Contains(getPipelineISBsBody, `"errMsg":null`) {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				s.T().Fatalf("failed to get piplines/isbs: %v", ctx.Err())
			}
		default:
			time.Sleep(100 * time.Millisecond)
			getPipelineISBsBody = HTTPExpect(s.T(), "https://localhost:8146").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/isbs", Namespace, pipelineName)).
				Expect().
				Status(200).Body().Raw()
		}
	}

	assert.Contains(s.T(), getPipelineISBsBody, `"bufferName":"numaflow-system-simple-pipeline-p1-0"`)
	assert.Contains(s.T(), getPipelineISBsBody, `"bufferName":"numaflow-system-simple-pipeline-output-0"`)

	getPipelineBody := HTTPExpect(s.T(), "https://localhost:8146").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), getPipelineBody, `"name":"simple-pipeline"`)
	assert.Contains(s.T(), getPipelineBody, `"status":"healthy"`)

	getPipelineWatermarksBody := HTTPExpect(s.T(), "https://localhost:8146").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/watermarks", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	AssertJsonExists(s.T(), getPipelineWatermarksBody, `data.#(edge=="input-p1").watermarks`)
	AssertJsonExists(s.T(), getPipelineWatermarksBody, `data.#(edge=="p1-output").watermarks`)

	getVerticesMetricsBody := HTTPExpect(s.T(), "https://localhost:8146").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/metrics", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	AssertJsonExists(s.T(), getVerticesMetricsBody, `data.input.#(vertex=="input").processingRates`)
	AssertJsonExists(s.T(), getVerticesMetricsBody, `data.p1.#(vertex=="p1").processingRates`)
	AssertJsonExists(s.T(), getVerticesMetricsBody, `data.output.#(vertex=="output").processingRates`)

	getVerticesPodsBody := HTTPExpect(s.T(), "https://localhost:8146").GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/input/pods", Namespace, pipelineName)).
		Expect().
		Status(200).Body().Raw()
	assert.Contains(s.T(), getVerticesPodsBody, `simple-pipeline-input-0`)

	// Call the DiscoverMetrics API for the vertex object
	discoverMetricsBodyForVertex := HTTPExpect(s.T(), "https://localhost:8146").GET("/api/v1/metrics-discovery/object/vertex").
		Expect().
		Status(200).Body().Raw()

	// Check that the response contains expected metrics for vertex object
	assert.Contains(s.T(), discoverMetricsBodyForVertex, "forwarder_data_read_total")

	// Call the API to get input vertex pods info
	getVertexPodsInfoBody := HTTPExpect(s.T(), "https://localhost:8146").
		GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/%s/pods-info", Namespace, pipelineName, "input")).
		Expect().
		Status(200).Body().Raw()

	// Check that the response contains expected pod details
	assert.Contains(s.T(), getVertexPodsInfoBody, `"name":`)                // Check for pod name
	assert.Contains(s.T(), getVertexPodsInfoBody, `"status":`)              // Check for pod status
	assert.Contains(s.T(), getVertexPodsInfoBody, `"totalCPU":`)            // Check for pod's cpu usage
	assert.Contains(s.T(), getVertexPodsInfoBody, `"totalMemory":`)         // Check for pod's memory usage
	assert.Contains(s.T(), getVertexPodsInfoBody, `"containerDetailsMap":`) // Check for pod's containers
}

func (s *APISuite) TestMetricsAPIsForMonoVertex() {
	w := s.Given().MonoVertex("@testdata/mono-vertex.yaml").
		When().
		CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	monoVertexName := "mono-vertex"

	defer w.UXServerPodPortForward(8149, 8443).TerminateAllPodPortForwards()

	w.Expect().MonoVertexPodsRunning()
	// Expect the messages to reach the sink.
	w.Expect().RedisSinkContains("mono-vertex", "199")
	w.Expect().RedisSinkContains("mono-vertex", "200")

	// Call the API to get mono vertex pods info
	getMonoVertexPodsInfoBody := HTTPExpect(s.T(), "https://localhost:8149").
		GET(fmt.Sprintf("/api/v1/namespaces/%s/mono-vertices/%s/pods-info", Namespace, monoVertexName)).
		Expect().
		Status(200).Body().Raw()

	// Check that the response contains expected pod details
	assert.Contains(s.T(), getMonoVertexPodsInfoBody, `"name":`)                // Check for pod name
	assert.Contains(s.T(), getMonoVertexPodsInfoBody, `"status":`)              // Check for pod status
	assert.Contains(s.T(), getMonoVertexPodsInfoBody, `"totalCPU":`)            // Check for pod's cpu usage
	assert.Contains(s.T(), getMonoVertexPodsInfoBody, `"totalMemory":`)         // Check for pod's memory usage
	assert.Contains(s.T(), getMonoVertexPodsInfoBody, `"containerDetailsMap":`) // Check for pod's containers

	// Call the DiscoverMetrics API for mono-vertex
	discoverMetricsBodyForMonoVertex := HTTPExpect(s.T(), "https://localhost:8149").GET("/api/v1/metrics-discovery/object/mono-vertex").
		Expect().
		Status(200).Body().Raw()

	// Check that the response contains expected metrics for mono-vertex
	assert.Contains(s.T(), discoverMetricsBodyForMonoVertex, "monovtx_processing_time_bucket")
	assert.Contains(s.T(), discoverMetricsBodyForMonoVertex, "monovtx_sink_time_bucket")
	assert.Contains(s.T(), discoverMetricsBodyForMonoVertex, "monovtx_read_total")
	assert.Contains(s.T(), discoverMetricsBodyForMonoVertex, "monovtx_pending")
}
