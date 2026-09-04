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
	"bytes"
	"fmt"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	. "github.com/numaproj/numaflow/test/fixtures"
)

type CronAutoscalingSuite struct {
	E2ESuite
}

// cronShortWindowFromNow returns a six-field (second minute hour dom month
// dow) start/end cron expression pair for a window that opens roughly
// startInSeconds from now and stays open for durationSeconds, anchored to the
// current wall clock.
func cronShortWindowFromNow(startInSeconds, durationSeconds int) (start, end string) {
	now := time.Now().UTC()
	from := now.Add(time.Duration(startInSeconds) * time.Second)
	to := from.Add(time.Duration(durationSeconds) * time.Second)
	start = fmt.Sprintf("%d %d %d * * *", from.Second(), from.Minute(), from.Hour())
	end = fmt.Sprintf("%d %d %d * * *", to.Second(), to.Minute(), to.Hour())
	return start, end
}

// renderCronTestdata renders the named testdata YAML template (relative to
// testdata/), substituting {{.Start}}/{{.End}} with the given cron
// expressions. Cron windows are anchored to wall-clock time, so they can't be
// baked into a static YAML fixture; the spec otherwise lives in testdata/ like
// every other e2e fixture.
func renderCronTestdata(t *testing.T, filename, start, end string) string {
	t.Helper()
	tmpl, err := template.ParseFiles("testdata/" + filename)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, struct{ Start, End string }{start, end}); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

// TestSourceVertexCronScaleUpFromZero verifies that a Pipeline source vertex
// with an active cron window scales up to the window's min replicas shortly
// after creation, even without any traffic/pending-message metrics being
// available.
func (s *CronAutoscalingSuite) TestSourceVertexCronScaleUpFromZero() {
	start, end := cronShortWindowFromNow(2, 60)
	spec := renderCronTestdata(s.T(), "cron-scale-up-pipeline.yaml", start, end)

	w := s.Given().Pipeline(spec).When().CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// The autoscaler should detect the active cron window and scale the
	// source vertex up to at least the window's min (3).
	w.Expect().VertexSizeScaledTo("input", 3)
}

// TestSourceVertexCronScaleDownAfterWindowExpires verifies that once an
// active cron window closes, the autoscaler stops honoring the window's
// bounds and reverts to base scale.min/scale.max, scaling a source vertex
// back down even though it had scaled up while the window was active.
func (s *CronAutoscalingSuite) TestSourceVertexCronScaleDownAfterWindowExpires() {
	// Window opens almost immediately and stays open long enough for the
	// autoscaler to reliably detect it and scale up to 3 before it closes on
	// its own while the test is still running.
	start, end := cronShortWindowFromNow(2, 60)
	spec := renderCronTestdata(s.T(), "cron-scale-down-pipeline.yaml", start, end)

	w := s.Given().Pipeline(spec).When().CreatePipelineAndWait()
	defer w.DeletePipelineAndWait()

	// Cron window opens shortly after creation; expect scale-up to 3.
	w.Expect().VertexSizeScaledTo("input", 3)

	// Window has now closed (it only lasted 60s); expect the autoscaler to
	// revert to base bounds and scale back down to max=1.
	w.Expect().VertexSizeScaledTo("input", 1)
}

// TestCronOnNonSourceVertexRejected verifies that configuring scale.cron on a
// non-source (sink) Pipeline vertex is rejected by the validating webhook,
// end to end, rather than silently ignored.
func (s *CronAutoscalingSuite) TestCronOnNonSourceVertexRejected() {
	start, end := cronShortWindowFromNow(2, 60)
	spec := renderCronTestdata(s.T(), "cron-non-source-rejected-pipeline.yaml", start, end)

	err := s.Given().CreatePipelineExpectingError(spec)
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "cron autoscaling is only supported for source vertices")
}

func TestCronAutoscalingSuite(t *testing.T) {
	suite.Run(t, new(CronAutoscalingSuite))
}
