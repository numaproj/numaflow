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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	daemonclient "github.com/numaproj/numaflow/pkg/daemon/client"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type UserDefinedSourceSuite struct {
	E2ESuite
}

func (s *UserDefinedSourceSuite) testSimpleSourceGo() {
	s.testSimpleSource("go", true)
}

func (s *UserDefinedSourceSuite) testSimpleSourceJava() {
	s.testSimpleSource("java", false)
}

func (s *UserDefinedSourceSuite) testSimpleSourcePython() {
	s.testSimpleSource("python", false)
}

func (s *UserDefinedSourceSuite) TestUDSource() {
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		s.testSimpleSourcePython()
	}()
	go func() {
		defer wg.Done()
		s.testSimpleSourceJava()
	}()
	go func() {
		defer wg.Done()
		s.testSimpleSourceGo()
	}()
	wg.Wait()
}

func (s *UserDefinedSourceSuite) testSimpleSource(lang string, verifyRate bool) {
	w := s.Given().Pipeline(fmt.Sprintf("@testdata/simple-source-%s.yaml", lang)).
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	pipelineName := fmt.Sprintf("simple-source-%s", lang)

	if verifyRate {
		// wait for all the pods and daemon server to come up
		w.Expect().VertexPodsRunning().DaemonPodsRunning().DaemonPodLogContains(pipelineName, LogDaemonStarted)
		// port-forward daemon server
		defer w.DaemonPodPortForward(pipelineName, 1234, dfv1.DaemonServicePort).
			TerminateAllPodPortForwards()
	} else {
		// wait for all the pods to come up
		w.Expect().VertexPodsRunning()
	}

	// we use the log sink instead of redis to verify the output because the simple user-defined source generates
	// such a large amount of data that the redis sink is not able to handle it, it breaks with OOM error

	// the user-defined simple source sends the read index of the message as the message content
	// verify the sink gets the first batch of data(0-499) - checking for some random numbers
	w.Expect().VertexPodLogContains("out", "147")
	w.Expect().VertexPodLogContains("out", "258")
	w.Expect().VertexPodLogContains("out", "369")
	// verify the sink get the second batch of data(500-999)
	w.Expect().VertexPodLogContains("out", "520")
	w.Expect().VertexPodLogContains("out", "630")
	w.Expect().VertexPodLogContains("out", "999")

	if verifyRate {
		// verify the processing rate match between source and sink
		client, err := daemonclient.NewDaemonServiceClient("localhost:1234")
		assert.NoError(s.T(), err)
		defer func() {
			_ = client.Close()
		}()

		// timeout the test if rates don't match within 2 minutes.
		timer := time.NewTimer(120 * time.Second)
		// we use 10-second windows for rate calculation
		// wait for 10 seconds for a new timestamped count entry to be added to the rate calculation windows
		waitInterval := 10 * time.Second
		succeedChan := make(chan struct{})
		go func() {
			vertexNames := []string{"in", "out"}
			for {
				var rates []float64
				for _, vertexName := range vertexNames {
					m, err := client.GetVertexMetrics(context.Background(), pipelineName, vertexName)
					assert.NoError(s.T(), err)
					assert.Equal(s.T(), pipelineName, *m[0].Pipeline)
					oneMinRate := m[0].ProcessingRates["1m"]
					rates = append(rates, oneMinRate)
				}
				if !ratesMatch(rates) {
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
			assert.Fail(s.T(), "timed out waiting for processing rate to match across vertices.")
		}
		timer.Stop()
	}
}

func ratesMatch(rates []float64) bool {
	if len(rates) <= 1 {
		return true
	}
	firstVal := rates[0]
	// the simple source can reach 8k TPS, we don't compare until the pipeline is stable.
	// using 5000 as a threshold
	if firstVal < 5000 {
		return false
	}
	for i := 1; i < len(rates); i++ {
		diff := math.Abs(firstVal - rates[i])
		if diff > (firstVal * 0.1) {
			return false
		}
	}
	return true
}

func TestUserDefinedSourceSuite(t *testing.T) {
	suite.Run(t, new(UserDefinedSourceSuite))
}
