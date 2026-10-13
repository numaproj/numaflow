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

package monovertex_e2e

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	dfv1 "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1"
	mvtxclient "github.com/numaproj/numaflow/pkg/mvtxdaemon/client"
	. "github.com/numaproj/numaflow/test/fixtures"
)

type MonoVertexSuite struct {
	E2ESuite
}

func runLocalStackCommand(args ...string) (string, error) {
	kubectlArgs := []string{"exec", "-n", Namespace, "deployment/localstack", "--", "awslocal"}
	kubectlArgs = append(kubectlArgs, args...)
	return Exec("kubectl", kubectlArgs...)
}

func requireLocalStackCommand(t *testing.T, args ...string) string {
	t.Helper()
	output, err := runLocalStackCommand(args...)
	if err != nil {
		t.Fatalf("LocalStack command failed: %v\n%s", err, output)
	}
	return strings.TrimSpace(output)
}

func createLocalStackQueue(t *testing.T, queueName string) string {
	t.Helper()
	return requireLocalStackCommand(
		t,
		"sqs", "create-queue",
		"--queue-name", queueName,
		"--query", "QueueUrl",
		"--output", "text",
	)
}

type localStackMessage struct {
	Body       string            `json:"Body"`
	Attributes map[string]string `json:"Attributes"`
}

func localStackQueueMessages(queueURL string) ([]localStackMessage, error) {
	output, err := runLocalStackCommand(
		"sqs", "receive-message",
		"--queue-url", queueURL,
		"--max-number-of-messages", "10",
		"--visibility-timeout", "0",
		"--attribute-names", "ApproximateReceiveCount",
		"--output", "json",
	)
	if err != nil {
		return nil, err
	}

	var response struct {
		Messages []localStackMessage `json:"Messages"`
	}
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		return nil, fmt.Errorf("parse received SQS messages %q: %w", output, err)
	}
	return response.Messages, nil
}

func findLocalStackMessage(messages []localStackMessage, body string) *localStackMessage {
	for i := range messages {
		if messages[i].Body == body {
			return &messages[i]
		}
	}
	return nil
}

func localStackQueueDepth(queueURL string) (visible int, notVisible int, err error) {
	output, err := runLocalStackCommand(
		"sqs", "get-queue-attributes",
		"--queue-url", queueURL,
		"--attribute-names", "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
		"--output", "json",
	)
	if err != nil {
		return 0, 0, err
	}

	var response struct {
		Attributes map[string]string `json:"Attributes"`
	}
	if err := json.Unmarshal([]byte(output), &response); err != nil {
		return 0, 0, fmt.Errorf("parse SQS queue attributes %q: %w", output, err)
	}

	visible, err = strconv.Atoi(response.Attributes["ApproximateNumberOfMessages"])
	if err != nil {
		return 0, 0, fmt.Errorf("parse visible SQS message count: %w", err)
	}
	notVisible, err = strconv.Atoi(response.Attributes["ApproximateNumberOfMessagesNotVisible"])
	if err != nil {
		return 0, 0, fmt.Errorf("parse in-flight SQS message count: %w", err)
	}
	return visible, notVisible, nil
}

func localStackRedriveAttributes(deadLetterQueueARN string) string {
	return fmt.Sprintf(
		`{"RedrivePolicy":"{\"deadLetterTargetArn\":\"%s\",\"maxReceiveCount\":\"2\"}"}`,
		deadLetterQueueARN,
	)
}

func setupLocalStack(t *testing.T) func() {
	t.Helper()
	output, err := Exec("kubectl", "apply", "-k", "../../config/apps/localstack", "-n", Namespace)
	if err != nil {
		t.Fatalf("failed to deploy LocalStack: %v\n%s", err, output)
	}

	cleanup := func() {
		output, err := Exec(
			"kubectl", "delete", "-k", "../../config/apps/localstack",
			"-n", Namespace, "--ignore-not-found=true",
		)
		assert.NoError(t, err, output)
	}

	ready := assert.Eventually(t, func() bool {
		output, err := Exec(
			"kubectl", "get", "deployment/localstack",
			"-n", Namespace,
			"-o", "jsonpath={.status.availableReplicas}",
		)
		return err == nil && strings.TrimSpace(output) == "1"
	}, 3*time.Minute, 2*time.Second, "LocalStack did not become ready")
	if !ready {
		cleanup()
		t.FailNow()
	}

	return cleanup
}

func (s *MonoVertexSuite) TestSQSNativeDeadLetterQueue() {
	cleanupLocalStack := setupLocalStack(s.T())
	defer cleanupLocalStack()

	ordersSourceURL := createLocalStackQueue(s.T(), "orders-source")
	refundsSourceURL := createLocalStackQueue(s.T(), "refunds-source")
	ordersDLQURL := createLocalStackQueue(s.T(), "orders-dlq")
	refundsDLQURL := createLocalStackQueue(s.T(), "refunds-dlq")

	ordersDLQARN := requireLocalStackCommand(
		s.T(),
		"sqs", "get-queue-attributes",
		"--queue-url", ordersDLQURL,
		"--attribute-names", "QueueArn",
		"--query", "Attributes.QueueArn",
		"--output", "text",
	)
	refundsDLQARN := requireLocalStackCommand(
		s.T(),
		"sqs", "get-queue-attributes",
		"--queue-url", refundsDLQURL,
		"--attribute-names", "QueueArn",
		"--query", "Attributes.QueueArn",
		"--output", "text",
	)

	// Queue ownership is external to Numaflow. Configure both policies before
	// creating the MonoVertex; startup should only read and validate them.
	requireLocalStackCommand(
		s.T(),
		"sqs", "set-queue-attributes",
		"--queue-url", ordersSourceURL,
		"--attributes", localStackRedriveAttributes(ordersDLQARN),
	)
	requireLocalStackCommand(
		s.T(),
		"sqs", "set-queue-attributes",
		"--queue-url", refundsSourceURL,
		"--attributes", localStackRedriveAttributes(refundsDLQARN),
	)

	w := s.Given().MonoVertex("@testdata/sqs-native-dlq.yaml").
		When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()
	w.Expect().MonoVertexPodsRunning()

	testRunID := time.Now().UnixNano()
	ordersMessage := fmt.Sprintf("orders-poison-message-%d", testRunID)
	refundsMessage := fmt.Sprintf("refunds-poison-message-%d", testRunID)
	requireLocalStackCommand(
		s.T(),
		"sqs", "send-message",
		"--queue-url", ordersSourceURL,
		"--message-body", ordersMessage,
	)
	requireLocalStackCommand(
		s.T(),
		"sqs", "send-message",
		"--queue-url", refundsSourceURL,
		"--message-body", refundsMessage,
	)

	// maxReceiveCount=2 means each poison message must be delivered and NACKed
	// twice before SQS transfers it to its configured DLQ.
	w.Expect().MonoVertexPodLogContains(
		LogReceivedNack,
		PodLogCheckOptionWithContainer("numa"),
		PodLogCheckOptionWithCount(4),
	)

	var ordersDLQMessage *localStackMessage
	ordersArrived := assert.Eventually(s.T(), func() bool {
		var err error
		messages, err := localStackQueueMessages(ordersDLQURL)
		if err != nil {
			s.T().Logf("waiting for orders DLQ: %v", err)
			return false
		}
		ordersDLQMessage = findLocalStackMessage(messages, ordersMessage)
		return ordersDLQMessage != nil
	}, 2*time.Minute, time.Second, "orders message did not reach orders-dlq")
	if !ordersArrived {
		s.T().FailNow()
	}

	var refundsDLQMessage *localStackMessage
	refundsArrived := assert.Eventually(s.T(), func() bool {
		var err error
		messages, err := localStackQueueMessages(refundsDLQURL)
		if err != nil {
			s.T().Logf("waiting for refunds DLQ: %v", err)
			return false
		}
		refundsDLQMessage = findLocalStackMessage(messages, refundsMessage)
		return refundsDLQMessage != nil
	}, 2*time.Minute, time.Second, "refunds message did not reach refunds-dlq")
	if !refundsArrived {
		s.T().FailNow()
	}

	ordersReceiveCount, err := strconv.Atoi(ordersDLQMessage.Attributes["ApproximateReceiveCount"])
	assert.NoError(s.T(), err)
	assert.GreaterOrEqual(s.T(), ordersReceiveCount, 2)
	refundsReceiveCount, err := strconv.Atoi(refundsDLQMessage.Attributes["ApproximateReceiveCount"])
	assert.NoError(s.T(), err)
	assert.GreaterOrEqual(s.T(), refundsReceiveCount, 2)

	assert.Eventually(s.T(), func() bool {
		ordersVisible, ordersNotVisible, ordersErr := localStackQueueDepth(ordersSourceURL)
		refundsVisible, refundsNotVisible, refundsErr := localStackQueueDepth(refundsSourceURL)
		return ordersErr == nil && refundsErr == nil &&
			ordersVisible == 0 && ordersNotVisible == 0 &&
			refundsVisible == 0 && refundsNotVisible == 0
	}, 30*time.Second, time.Second, "source queues were not drained after native SQS redrive")

	assert.NotEqual(s.T(), ordersDLQMessage.Body, refundsMessage)
	assert.NotEqual(s.T(), refundsDLQMessage.Body, ordersMessage)
}

func (s *MonoVertexSuite) TestMonoVertexWithAllContainers() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-with-all-containers.yaml").
		When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().MonoVertexPodsRunning().MvtxDaemonPodsRunning()

	defer w.MonoVertexPodPortForward(8931, dfv1.MonoVertexMetricsPort).
		MvtxDaemonPodPortForward(3232, dfv1.MonoVertexDaemonServicePort).
		TerminateAllPodPortForwards()

	// Check metrics endpoints
	HTTPExpect(s.T(), "https://localhost:8931").GET("/metrics").
		Expect().
		Status(200)

	HTTPExpect(s.T(), "https://localhost:3232").GET("/metrics").
		Expect().
		Status(200)

	// Expect the messages to be processed by the transformer.
	w.Expect().MonoVertexPodLogContains("AssignEventTime", PodLogCheckOptionWithContainer("transformer"))

	// Simulate primary sink failure and check fallback sink
	w.Expect().MonoVertexPodLogContains("Primary sink under maintenance", PodLogCheckOptionWithContainer("udsink"))

	// Expect the messages to reach the fallback sink.
	w.Expect().RedisSinkContains("fallback-sink-key", "1000")
	w.Expect().RedisSinkContains("fallback-sink-key", "1001")

}

func (s *MonoVertexSuite) TestExponentialBackoffRetryStrategy() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-exponential-retry-strategy.yaml").When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()
	w.Expect().MonoVertexPodsRunning()
	w.Expect().MonoVertexPodLogContains(LogRetryAttempt+`"1"`, PodLogCheckOptionWithContainer("numa"))
	w.Expect().MonoVertexPodLogContains(LogRetryAttempt+`"2"`, PodLogCheckOptionWithContainer("numa"))
	w.Expect().MonoVertexPodLogContains(LogRetriesExhaustedDrop, PodLogCheckOptionWithContainer("numa"))
	w.Expect().MonoVertexPodLogNotContains(LogRetryAttempt+`"3"`, PodLogCheckOptionWithContainer("numa"), PodLogCheckOptionWithTimeout(time.Second))
}

func (s *MonoVertexSuite) TestMonoVertexRateLimitWithRedisStore() {
	w := s.Given().MonoVertex("@testdata/mono-vertex-rate-limit-redis.yaml").
		When().
		CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().
		MonoVertexPodsRunning().
		MvtxDaemonPodsRunning()

	// port-forward mvtx daemon server
	defer w.MvtxDaemonPodPortForward(1234, dfv1.MonoVertexDaemonServicePort).
		TerminateAllPodPortForwards()

	// Verify rate limiting is working by checking processing rates
	client, err := mvtxclient.NewGRPCClient("localhost:1234")
	assert.NoError(s.T(), err)
	defer func() {
		_ = client.Close()
	}()

	w.Expect().MonoVertexPodLogContains("\"processed\":\"50", PodLogCheckOptionWithContainer("numa"), PodLogCheckOptionWithCount(20))
}

func (s *MonoVertexSuite) TestMonoVertexUserMetadataPropagation() {
	w := s.Given().MonoVertex("@testdata/metadata-monovertex.yaml").
		When().
		CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	w.Expect().MonoVertexPodsRunning()

	w.Expect().
		MonoVertexPodLogContains("Groups at mapper:", PodLogCheckOptionWithContainer("udf")).
		MonoVertexPodLogContains("event-time-group", PodLogCheckOptionWithContainer("udf")).
		MonoVertexPodLogContains("simple-source", PodLogCheckOptionWithContainer("udf"))

	w.Expect().
		MonoVertexPodLogContains("User Metadata:", PodLogCheckOptionWithContainer("udsink")).
		MonoVertexPodLogContains("event-time-group", PodLogCheckOptionWithContainer("udsink")).
		MonoVertexPodLogContains("simple-source", PodLogCheckOptionWithContainer("udsink")).
		MonoVertexPodLogContains("map-group", PodLogCheckOptionWithContainer("udsink")).
		MonoVertexPodLogContains("txn-id", PodLogCheckOptionWithContainer("udsink"))
}

func (s *MonoVertexSuite) TestMapBypass() {

	w := s.Given().MonoVertex("@testdata/map-bypass.yaml").
		When().
		CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()
	monoVertexName := "map-bypass"

	// wait for all the pods to come up
	w.Expect().MonoVertexPodsRunning()

	// send a message that should be routed to the on success sink
	w.SendMessageToMvTx(monoVertexName, NewHttpPostRequest().WithBody([]byte("on-success-message")))
	// send a message that should be routed to the fallback sink
	w.SendMessageToMvTx(monoVertexName, NewHttpPostRequest().WithBody([]byte("fallback-message")))
	// send a message that should be routed to the primary sink
	w.SendMessageToMvTx(monoVertexName, NewHttpPostRequest().WithBody([]byte("primary-message")))

	w.Expect().RedisSinkContains("bypass-on-success-output", "on-success-message")
	w.Expect().RedisSinkContains("bypass-fallback-output", "fallback-message")
	w.Expect().RedisSinkContains("bypass-sink-output", "primary-message")
}

func (s *MonoVertexSuite) TestSourceTransformerBypass() {
	w := s.Given().MonoVertex("@testdata/source-transformer-bypass.yaml").
		When().
		CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()
	monoVertexName := "st-bypass"

	// wait for all the pods to come up
	w.Expect().MonoVertexPodsRunning()

	// send a message that should be routed to the on success sink
	w.SendMessageToMvTx(monoVertexName, NewHttpPostRequest().WithBody([]byte("on-success-message")))
	// send a message that should be routed to the fallback sink
	w.SendMessageToMvTx(monoVertexName, NewHttpPostRequest().WithBody([]byte("fallback-message")))
	// send a message that should be routed to the primary sink
	w.SendMessageToMvTx(monoVertexName, NewHttpPostRequest().WithBody([]byte("primary-message")))

	w.Expect().RedisSinkContains("bypass-on-success-output", "on-success-message")
	w.Expect().RedisSinkContains("bypass-fallback-output", "fallback-message")
	w.Expect().RedisSinkContains("bypass-sink-output", "primary-message")
}

func (s *MonoVertexSuite) TestStreamingMonoVertex() {
	w := s.Given().MonoVertex("@testdata/streaming-mono-vertex.yaml").
		When().CreateMonoVertexAndWait()
	defer w.DeleteMonoVertexAndWait()

	monoVertexName := "streaming-mono-vertex"

	// wait for all the pods to come up
	w.Expect().MonoVertexPodsRunning()

	// Send N=5 distinct messages through the HTTP source.
	// Each message has a unique payload so we can assert individually that
	// all N reached the sink (at-least-once: count >= 1 per message).
	messages := []string{
		"stream-msg-0",
		"stream-msg-1",
		"stream-msg-2",
		"stream-msg-3",
		"stream-msg-4",
	}
	for _, msg := range messages {
		w.SendMessageToMvTx(monoVertexName, NewHttpPostRequest().WithBody([]byte(msg)))
	}

	// Assert all N messages reached the sink. The Rust redis-sink stores each
	// message payload as a hash field (HINCRBY), so count >= 1 confirms
	// at-least-once delivery even if a message was redelivered.
	for _, msg := range messages {
		w.Expect().RedisSinkContains("streaming-mvtx-output", msg)
	}
}

func TestMonoVertexSuite(t *testing.T) {
	suite.Run(t, new(MonoVertexSuite))
}
