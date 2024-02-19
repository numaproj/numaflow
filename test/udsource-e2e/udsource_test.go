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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type UserDefinedSourceSuite struct {
	E2ESuite
}

func (s *UserDefinedSourceSuite) testSimpleSourceGo() {
	s.testSimpleSource("go")
}

func (s *UserDefinedSourceSuite) testSimpleSourceJava() {
	s.testSimpleSource("java")
}

func (s *UserDefinedSourceSuite) testSimpleSourcePython() {
	s.testSimpleSource("python")
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

func (s *UserDefinedSourceSuite) testSimpleSource(lang string) {
	w := s.Given().Pipeline(fmt.Sprintf("@testdata/simple-source-%s.yaml", lang)).
		When().
		CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// wait for all the pods to come up
	w.Expect().VertexPodsRunning()

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

}

func TestUserDefinedSourceSuite(t *testing.T) {
	suite.Run(t, new(UserDefinedSourceSuite))
}
