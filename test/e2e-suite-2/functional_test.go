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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type FunctionalSuite struct {
	E2ESuite
}

func (s *FunctionalSuite) TestDropOnFull() {
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

	expectedDropMetric := `forwarder_drop_total{buffer="numaflow-system-drop-on-full-in-sink",pipeline="drop-on-full",vertex="in"} 1`
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

func TestFunctionalSuite(t *testing.T) {
	suite.Run(t, new(FunctionalSuite))
}
