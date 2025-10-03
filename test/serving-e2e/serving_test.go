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

package serving_e2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/numaproj/numaflow/test/fixtures"
)

//go:generate kubectl -n numaflow-system delete -f testdata/serving-pipeline-cat.yaml --ignore-not-found=true
//go:generate kubectl -n numaflow-system wait --for=condition=ready pod -l app.kubernetes.io/name=controller-manager --timeout 30s

const Namespace = "numaflow-system"

type ServingSuite struct {
	fixtures.E2ESuite
}

// {"message":"Successfully published message","id":"0195859e-b05f-78d3-b0b6-6946a77f842e","code":200,"timestamp":"2025-03-11T14:32:05.455629106Z"}
type asyncAPIResponse struct {
	Message   string    `json:"message"`
	Id        string    `json:"id"`
	Code      int       `json:"code"`
	Timestamp time.Time `json:"timestamp"`
}

func (resp *asyncAPIResponse) validate(expReqId string) error {
	if resp.Message != "Successfully published message" {
		return fmt.Errorf("message field = %q, expected='Successfully published message'", resp.Message)
	}
	if resp.Id != expReqId {
		return fmt.Errorf("value of id field should be %q, current value: %q", expReqId, resp.Id)
	}
	var defaultTime time.Time
	if resp.Timestamp == defaultTime {
		return fmt.Errorf("invalid value for timestamp field: %q", resp.Timestamp)
	}
	if resp.Code != 200 {
		return fmt.Errorf("expected value of 'code' field is 200, got %d", resp.Code)
	}
	return nil
}

func (ss *ServingSuite) TestServingSource() {
	w := ss.Given().ServingPipeline("@testdata/serving-pipeline-cat.yaml").
		When().
		CreateServingPipelineAndWait()
	defer w.DeleteServingPipelineAndWait()

	w.Expect().ServingPodsRunning()

	w.StreamServingPodLogs("numa")
	defer w.TerminateAllPodLogs()

	servingPipelineName := "serving-source"
	serviceName := "serving-source-serving"
	// Check Service
	cmd := fmt.Sprintf("kubectl -n %s get svc -lnumaflow.numaproj.io/serving-pipeline-name=%s | grep -v CLUSTER-IP | grep -v headless", Namespace, servingPipelineName)
	w.Exec("sh", []string{"-c", cmd}, fixtures.OutputRegexp(serviceName))

	// Send a request using sync API
	syncResp := fixtures.SendServingMessage(serviceName, "", "test data", true)
	assert.Equal(ss.T(), "test data", syncResp)

	// Send a request using async API
	const reqId = "req-12345"
	asyncRespText := fixtures.SendServingMessage(serviceName, reqId, "test data", false)
	var asyncResp asyncAPIResponse
	err := json.Unmarshal([]byte(asyncRespText), &asyncResp)
	require.NoError(ss.T(), err)
	require.NoError(ss.T(), asyncResp.validate(reqId))

	// Use fetch API to retrieve the results of the above async request
	attempt := 0
	var asyncResult string
	for {
		if attempt > 5 {
			ss.T().Fatalf("Processing for async request is still in-progress")
			break
		}
		asyncResult = fixtures.FetchServingResult(serviceName, reqId)
		if asyncResult != `{"status":"in-progress"}` {
			break
		}
		attempt++
		time.Sleep(2 * time.Second)
	}
	assert.Equal(ss.T(), "test data", asyncResult)
}

func TestServingSuite(t *testing.T) {
	suite.Run(t, new(ServingSuite))
}
