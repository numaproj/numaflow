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

package sideinput_e2e

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type SideInputUDSSuite struct {
	E2ESuite
}

func (s *SideInputUDSSuite) setUpTests(pipeLineFile string) *When {
	w := s.Given().Pipeline(pipeLineFile).When().CreatePipelineAndWait()
	w.Expect().VertexPodsRunning()
	return w
}

func (s *SideInputUDSSuite) TestSinkWithSideInput() {
	// the side inputs feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		s.T().SkipNow()
	}

	w := s.setUpTests("@testdata/sideinput-sink.yaml")
	defer w.DeletePipelineAndWait()
	w.Expect().RedisSinkContains("sideinput-sink-test-redis-uds", "e2e-even", SinkCheckWithTimeout(2*time.Minute))
}

func (s *SideInputUDSSuite) TestSourceWithSideInput() {
	// the side inputs feature is not supported with redis ISBSVC
	if strings.ToUpper(os.Getenv("ISBSVC")) == "REDIS" {
		s.T().SkipNow()
	}

	w := s.setUpTests("@testdata/sideinput-source.yaml")
	defer w.DeletePipelineAndWait()
	w.Expect().RedisSinkContains("sideinput-source-test-redis-uds", "e2e-even", SinkCheckWithTimeout(2*time.Minute))
}

func TestSideInputUDSSuite(t *testing.T) {
	suite.Run(t, new(SideInputUDSSuite))
}
