//go:build test

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

package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	. "github.com/numaproj/numaflow/test/fixtures"
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
		VertexPodLogContains("p1", LogUDFVertexStarted, PodLogCheckOptionWithContainer("numa")).
		VertexPodLogContains("output", SinkVertexStarted).
		DaemonPodLogContains(pipelineName, LogDaemonStarted).
		VertexPodLogContains("output", `"Data":.*,"Createdts":.*`)

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
		GET(fmt.Sprintf("/api/v1/pipelines/%s/buffers/%s", pipelineName, dfv1.GenerateBufferName(Namespace, pipelineName, "p1", 0))).
		Expect().
		Status(200).Body().Contains("pipeline")

	HTTPExpect(s.T(), "https://localhost:1234").
		GET(fmt.Sprintf("/api/v1/pipelines/%s/vertices/%s/metrics", pipelineName, "p1")).
		Expect().
		Status(200).Body().Contains("pipeline")

	HTTPExpect(s.T(), "https://localhost:1234").GET("/readyz").
		Expect().
		Status(204)

	HTTPExpect(s.T(), "https://localhost:1234").GET("/livez").
		Expect().
		Status(204)

	// Test Daemon service with gRPC
	client, err := daemonclient.NewDaemonServiceClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()
	buffers, err := client.ListPipelineBuffers(context.Background(), pipelineName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 2, len(buffers))
	bufferInfo, err := client.GetPipelineBuffer(context.Background(), pipelineName, dfv1.GenerateBufferName(Namespace, pipelineName, "p1", 0))
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), pipelineName, *bufferInfo.Pipeline)
	m, err := client.GetVertexMetrics(context.Background(), pipelineName, "p1")
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), pipelineName, *m[0].Pipeline)

	// verify that the rate is calculated
	timer := time.NewTimer(120 * time.Second)
	// we use 10-second windows for rate calculation
	// wait for 10 seconds for a new timestamped count entry to be added to the rate calculation windows
	waitInterval := 10 * time.Second
	succeedChan := make(chan struct{})
	go func() {
		vertexNames := []string{"input", "p1", "output"}
		for {
			accurateCount := 0
			for _, vertexName := range vertexNames {
				m, err := client.GetVertexMetrics(context.Background(), pipelineName, vertexName)
				assert.NoError(s.T(), err)
				assert.Equal(s.T(), pipelineName, *m[0].Pipeline)
				oneMinRate := m[0].ProcessingRates["1m"]
				// the rate should be around 5
				if oneMinRate > 4 || oneMinRate < 6 {
					accurateCount++
				}
			}
			if accurateCount != len(vertexNames) {
				time.Sleep(waitInterval)
			} else {
				succeedChan <- struct{}{}
				break
			}
		}
	}()
	select {
	case <-succeedChan:
		time.Sleep(waitInterval)
		break
	case <-timer.C:
		assert.Fail(s.T(), "timed out waiting for rate to be calculated")
	}
	timer.Stop()
}

func (s *FunctionalSuite) TestUDFFiltering() {
	w := s.Given().Pipeline("@testdata/udf-filtering.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "udf-filtering"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	expect0 := `{"id": 180, "msg": "hello", "expect0": "fail", "desc": "A bad example"}`
	expect1 := `{"id": 80, "msg": "hello1", "expect1": "fail", "desc": "A bad example"}`
	expect2 := `{"id": 80, "msg": "hello", "expect2": "fail", "desc": "A bad example"}`
	expect3 := `{"id": 80, "msg": "hello", "expect3": "succeed", "desc": "A good example"}`
	expect4 := `{"id": 80, "msg": "hello", "expect4": "succeed", "desc": "A good example"}`

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect0))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect1))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect2))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect3))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte(expect4)))

	w.Expect().SinkContains("out", expect3)
	w.Expect().SinkContains("out", expect4)
	w.Expect().SinkNotContains("out", expect0)
	w.Expect().SinkNotContains("out", expect1)
	w.Expect().SinkNotContains("out", expect2)
}

func (s *FunctionalSuite) TestConditionalForwarding() {
	w := s.Given().Pipeline("@testdata/even-odd.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "even-odd"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("888888"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("888889"))).
		SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("not an integer")))

	w.Expect().SinkContains("even-sink", "888888")
	w.Expect().SinkNotContains("even-sink", "888889")
	w.Expect().SinkNotContains("even-sink", "not an integer")

	w.Expect().SinkContains("odd-sink", "888889")
	w.Expect().SinkNotContains("odd-sink", "888888")
	w.Expect().SinkNotContains("odd-sink", "not an integer")

	w.Expect().SinkContains("number-sink", "888888")
	w.Expect().SinkContains("number-sink", "888889")
	w.Expect().SinkNotContains("number-sink", "not an integer")
}

func (s *FunctionalSuite) TestDropOnFull() {

	// the drop on full feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		s.T().SkipNow()
	}

	w := s.Given().Pipeline("@testdata/drop-on-full.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "drop-on-full"

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()
	defer w.VertexPodPortForward("in", 8001, dfv1.VertexMetricsPort).
		TerminateAllPodPortForwards()

	// scale the sinks down to 0 pod to create a buffer full scenario.
	scaleDownArgs := "kubectl scale vtx drop-on-full-sink --replicas=0 -n numaflow-system"
	w.Exec("/bin/sh", []string{"-c", scaleDownArgs}, CheckVertexScaled)
	w.Expect().VertexSizeScaledTo("sink", 0)

	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("1")))
	// give buffer writer some time to update the isFull attribute.
	// 5s is a carefully chosen number to create a stable buffer full scenario.
	time.Sleep(time.Second * 5)
	w.SendMessageTo(pipelineName, "in", NewHttpPostRequest().WithBody([]byte("2")))

	expectedDropMetric := `forwarder_drop_total{partition_name="numaflow-system-drop-on-full-sink-0",pipeline="drop-on-full",replica="0",vertex="in",vertex_type="Source"} 1`
	// wait for the drop metric to be updated, time out after 10s.
	timeoutChan := time.After(time.Second * 10)
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			metricsString := HTTPExpect(s.T(), "https://localhost:8001").GET("/metrics").
				Expect().
				Status(200).Body().Raw()
			if strings.Contains(metricsString, expectedDropMetric) {
				return
			}
		case <-timeoutChan:
			s.T().Fatalf("timeout waiting for metrics to be updated")
		}
	}
}

func (s *FunctionalSuite) TestWatermarkEnabled() {

	// the watermark feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		s.T().SkipNow()
	}

	w := s.Given().Pipeline("@testdata/watermark.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := "simple-pipeline-watermark"
	// TODO: Any way to extract the list from suite
	edgeList := []string{"input-cat1", "input-cat2", "cat1-output1", "cat2-cat3", "cat3-output2"}

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning().DaemonPodsRunning()

	defer w.DaemonPodPortForward(pipelineName, 1234, dfv1.DaemonServicePort).
		TerminateAllPodPortForwards()

	// Test Daemon service with gRPC
	client, err := daemonclient.NewDaemonServiceClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()
	buffers, err := client.ListPipelineBuffers(context.Background(), pipelineName)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), 8, len(buffers))
	bufferInfo, err := client.GetPipelineBuffer(context.Background(), pipelineName, dfv1.GenerateBufferName(Namespace, pipelineName, "cat1", 0))
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), pipelineName, *bufferInfo.Pipeline)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	isProgressing, err := isWatermarkProgressing(ctx, client, pipelineName, edgeList, 3)
	assert.NoError(s.T(), err, "TestWatermarkEnabled failed %s\n", err)
	assert.Truef(s.T(), isProgressing, "isWatermarkProgressing\n")
}

// isWatermarkProgressing checks whether the watermark for each edge in a pipeline is progressing monotonically.
// progressCount is the number of progressions the watermark value should undertake within the timeout deadline for it
func isWatermarkProgressing(ctx context.Context, client *daemonclient.DaemonClient, pipelineName string, edgeList []string, progressCount int) (bool, error) {
	prevWatermark := make([]int64, len(edgeList))
	for i := 0; i < len(edgeList); i++ {
		prevWatermark[i] = -1
	}
	for i := 0; i < progressCount; i++ {
		currentWatermark := prevWatermark
		for func(current []int64, prev []int64) bool {
			for j := 0; j < len(current); j++ {
				if current[j] > prev[j] {
					return false
				}
			}
			return true
		}(currentWatermark, prevWatermark) {
			wm, err := client.GetPipelineWatermarks(ctx, pipelineName)
			if err != nil {
				return false, err
			}
			pipelineWatermarks := make([]int64, len(edgeList))
			idx := 0
			for _, e := range wm {
				pipelineWatermarks[idx] = e.Watermarks[0]
				idx++
			}
			currentWatermark = pipelineWatermarks
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
		prevWatermark = currentWatermark
	}
	return true, nil
}

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
