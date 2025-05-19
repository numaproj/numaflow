package retry_strategy_e2e

import (
	"strings"
	"testing"
	"time"

	. "github.com/numaproj/numaflow/test/fixtures"
	"github.com/stretchr/testify/suite"
)

type RetryStrategySuite struct {
	E2ESuite
}

func (r *RetryStrategySuite) TestExponentialBackoffRetryStrategy() {
	w := r.Given().MonoVertex("@testdata/exponential/mono-vertex-exponential-retry-strategy.yaml").When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	// w.Expect().MonoVertexPodLogContains("Retry attempt 1 due to retryable error. Errors", PodLogCheckOptionWithContainer("numa"))
	// w.Expect().MonoVertexPodLogContains("Retry attempt 2 due to retryable error. Errors", PodLogCheckOptionWithContainer("numa"))
	// w.Expect().MonoVertexPodLogContains("Retry attempt 3 due to retryable error. Errors", PodLogCheckOptionWithContainer("numa"))
	// // max attempts would be 3, as steps = 3
	// w.Expect().MonoVertexPodLogNotContains("Retry attempt 4 due to retryable error. Errors", PodLogCheckOptionWithContainer("numa"))

	// time.Sleep(10 * time.Second)
	logs := w.GetMonoVertexPodLogs("numa", true)
	r.T().Logf("logs: %s", logs)

	// var t1, t2, t4 time.Time
	// for line := range strings.SplitSeq(logs, "\n") {
	// 	if strings.Contains(line, "Retry attempt 1 due to retryable error. Errors") {
	// 		t1 = extractTimestamp(line)
	// 	}
	// 	if strings.Contains(line, "Retry attempt 2 due to retryable error. Errors") {
	// 		t2 = extractTimestamp(line)
	// 	}
	// 	// if strings.Contains(line, "Retry attempt 3 due to retryable error. Errors") {
	// 	// 	t3 = extractTimestamp(line)
	// 	// }
	// 	if strings.Contains(line, "Using onFailure Retry") {
	// 		t4 = extractTimestamp(line)
	// 	}
	// }
	// r.Require().False(t1.IsZero(), "Retry attempt 1 log not found or timestamp missing")
	// r.Require().False(t2.IsZero(), "Retry attempt 2 log not found or timestamp missing")
	// // r.Require().False(t3.IsZero(), "Retry attempt 3 log not found or timestamp missing")
	// r.Require().True(t2.Sub(t1) >= 1*time.Second, "Retry attempt 1 occurred at least after 1 second")
	// r.Require().True(t4.Sub(t2) < 2*time.Second, "Retry attempt 2 occurred before 2 sec, as cap is 1 sec")
	// // r.Require().True(t4.Sub(t3) < 3*time.Second, "Retry attempt 3 should take less than 3 seconds, as cap is 2sec")

}

func (r *RetryStrategySuite) TestDefaultRetryStrategy() {
	// r.testRetryStrategy("go")
}

func TestRetryStrategySuite(t *testing.T) {
	suite.Run(t, new(RetryStrategySuite))
}

// Helper to extract timestamp from log line, adjust the layout as per your log format
func extractTimestamp(line string) time.Time {
	// Example: "2024-05-19T12:34:56.789Z Retry attempt 1 due to retryable error. Errors"
	parts := strings.SplitN(line, " ", 2)
	if len(parts) < 2 {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339, parts[0])
	return t
}
