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

package redrive_e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	mvtxclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type RedriveSuite struct {
	E2ESuite
}

func (s *RedriveSuite) TestPipelineRuntimeErrorsFromUDFCrash() {
	w := s.Given().Pipeline("@testdata/runtime-error-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "runtime-error-pipeline"

	w.Expect().VertexPodsRunning().DaemonPodsRunning()

	defer w.VertexPodPortForward("p1", 8941, dfv1.VertexRuntimePort).
		DaemonPodPortForward(pipelineName, 1241, dfv1.DaemonServicePort).
		UXServerPodPortForward(8141, 8443).
		TerminateAllPodPortForwards()

	client, err := daemonclient.NewGRPCDaemonServiceClient("localhost:1241")
	assert.NoError(s.T(), err)
	defer func() { assert.NoError(s.T(), client.Close()) }()

	SendMessageTo(fmt.Sprintf("%s-in", pipelineName), "in", NewHttpPostRequest().WithBody([]byte("not-json")))

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8941").GET("/runtime/errors").
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udf"`)
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errors, err := client.GetVertexErrors(ctx, pipelineName, "p1")
		return err == nil && strings.Contains(fmt.Sprintf("%v", errors), "udf")
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8141").
			GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/%s/errors", Namespace, pipelineName, "p1")).
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udf"`)
	}, 2*time.Minute, time.Second)
}

func (s *RedriveSuite) TestPipelineRuntimeErrorsFromSinkCrash() {
	w := s.Given().Pipeline("@testdata/runtime-error-sink-pipeline.yaml").
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()
	pipelineName := "runtime-error-sink-pipeline"

	w.Expect().VertexPodsRunning().DaemonPodsRunning()

	defer w.VertexPodPortForward("out", 8942, dfv1.VertexRuntimePort).
		DaemonPodPortForward(pipelineName, 1242, dfv1.DaemonServicePort).
		UXServerPodPortForward(8142, 8443).
		TerminateAllPodPortForwards()

	client, err := daemonclient.NewGRPCDaemonServiceClient("localhost:1242")
	assert.NoError(s.T(), err)
	defer func() { assert.NoError(s.T(), client.Close()) }()

	SendMessageTo(fmt.Sprintf("%s-in", pipelineName), "in", NewHttpPostRequest().WithBody([]byte("trigger sink panic")))

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8942").GET("/runtime/errors").
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udsink"`)
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errors, err := client.GetVertexErrors(ctx, pipelineName, "out")
		return err == nil && strings.Contains(fmt.Sprintf("%v", errors), "udsink")
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8142").
			GET(fmt.Sprintf("/api/v1/namespaces/%s/pipelines/%s/vertices/%s/errors", Namespace, pipelineName, "out")).
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udsink"`)
	}, 2*time.Minute, time.Second)
}

func (s *RedriveSuite) TestMonoVertexRuntimeErrorsFromUDFCrash() {
	monoVertexName := "runtime-error-monovertex"
	w := s.Given().MonoVertex("@testdata/runtime-error-monovertex.yaml").
		When().CreateMonoVertexAndWait()
	defer w.Exec("kubectl", []string{"delete", "monovertices.numaflow.numaproj.io", monoVertexName, "-n", Namespace, "--ignore-not-found=true"}, OutputRegexp(""))

	w.Expect().MonoVertexPodsRunning().MvtxDaemonPodsRunning()

	defer w.MonoVertexPodPortForward(8943, dfv1.MonoVertexRuntimePort).
		MvtxDaemonPodPortForward(3243, dfv1.MonoVertexDaemonServicePort).
		UXServerPodPortForward(8143, 8443).
		TerminateAllPodPortForwards()

	client, err := mvtxclient.NewGRPCClient("localhost:3243")
	assert.NoError(s.T(), err)
	defer func() { assert.NoError(s.T(), client.Close()) }()

	go func() {
		defer func() {
			// The HTTP request can outlive the test because the UDF process exits before ACKing it.
			_ = recover()
		}()
		SendMessageTo(monoVertexName, monoVertexName, NewHttpPostRequest().WithBody([]byte("not-json")))
	}()

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8943").GET("/runtime/errors").
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udf"`)
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errors, err := client.GetMonoVertexErrors(ctx, monoVertexName)
		return err == nil && strings.Contains(fmt.Sprintf("%v", errors), "udf")
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8143").
			GET(fmt.Sprintf("/api/v1/namespaces/%s/mono-vertices/%s/errors", Namespace, monoVertexName)).
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udf"`)
	}, 2*time.Minute, time.Second)
}

func (s *RedriveSuite) TestMonoVertexRuntimeErrorsFromSinkCrash() {
	monoVertexName := "runtime-error-sink-monovertex"
	w := s.Given().MonoVertex("@testdata/runtime-error-sink-monovertex.yaml").
		When().CreateMonoVertexAndWait()
	defer w.Exec("kubectl", []string{"delete", "monovertices.numaflow.numaproj.io", monoVertexName, "-n", Namespace, "--ignore-not-found=true"}, OutputRegexp(""))

	w.Expect().MonoVertexPodsRunning().MvtxDaemonPodsRunning()

	defer w.MonoVertexPodPortForward(8944, dfv1.MonoVertexRuntimePort).
		MvtxDaemonPodPortForward(3244, dfv1.MonoVertexDaemonServicePort).
		UXServerPodPortForward(8144, 8443).
		TerminateAllPodPortForwards()

	client, err := mvtxclient.NewGRPCClient("localhost:3244")
	assert.NoError(s.T(), err)
	defer func() { assert.NoError(s.T(), client.Close()) }()

	go func() {
		defer func() {
			// The HTTP request can outlive the test because the sink process exits before ACKing it.
			_ = recover()
		}()
		SendMessageTo(monoVertexName, monoVertexName, NewHttpPostRequest().WithBody([]byte("trigger sink panic")))
	}()

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8944").GET("/runtime/errors").
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udsink"`)
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		errors, err := client.GetMonoVertexErrors(ctx, monoVertexName)
		return err == nil && strings.Contains(fmt.Sprintf("%v", errors), "udsink")
	}, 2*time.Minute, time.Second)

	assert.Eventually(s.T(), func() bool {
		body := HTTPExpect(s.T(), "https://localhost:8144").
			GET(fmt.Sprintf("/api/v1/namespaces/%s/mono-vertices/%s/errors", Namespace, monoVertexName)).
			Expect().
			Status(200).Body().Raw()
		return strings.Contains(body, `"container":"udsink"`)
	}, 2*time.Minute, time.Second)
}

func TestRedriveSuite(t *testing.T) {
	suite.Run(t, new(RedriveSuite))
}
