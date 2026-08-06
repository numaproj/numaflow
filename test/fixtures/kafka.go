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

package fixtures

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/rand"
)

func GenerateKafkaTopicName() string {
	return fmt.Sprintf("e2e-topic-%s", rand.String(5))
}

func CreateKafkaTopic(topicName string, partitions int32) {
	log.Printf("create Kafka topic %q\n", topicName)
	InvokeE2EAPI("/kafka/create-topic?topic=%s&partitions=%d", topicName, partitions)
}

func DeleteKafkaTopic(topic string) {
	log.Printf("delete Kafka topic %q\n", topic)
	InvokeE2EAPI("/kafka/delete-topic?topic=%s", topic)
}

func ListKafkaTopics() {
	InvokeE2EAPI("/kafka/list-topics")
}

func ResetKafkaClients() {
	InvokeE2EAPI("/kafka/reset")
}

func PumpKafkaTopic(topic string, n int, opts ...interface{}) {
	var sleep time.Duration
	var prefix string
	var size int
	for _, opt := range opts {
		switch v := opt.(type) {
		case time.Duration:
			sleep = v
		case string:
			prefix = v
		case int:
			size = v
		default:
			panic(fmt.Errorf("unexpected option type %T", opt))
		}
	}
	log.Printf("pumping Kafka topic %q sleeping %v with %d messages sized %d\n", topic, sleep, n, size)
	InvokeE2EAPI("/kafka/pump-topic?topic=%s&sleep=%v&n=%d&prefix=%s&size=%d", topic, sleep, n, prefix, size)
}

// PumpKafkaTopicPartitions pumps messages across all requested partitions in round-robin order.
func PumpKafkaTopicPartitions(topic string, n, partitions int, sleep time.Duration, size int) {
	log.Printf("pumping Kafka topic %q across %d partitions sleeping %v with %d messages sized %d\n", topic, partitions, sleep, n, size)
	InvokeE2EAPI(
		"/kafka/pump-topic?topic=%s&sleep=%v&n=%d&size=%d&partitions=%d",
		topic,
		sleep,
		n,
		size,
		partitions,
	)
}

// PumpKafkaTopicPartitionsAsync starts a cancelable partitioned pump and reports completion.
func PumpKafkaTopicPartitionsAsync(ctx context.Context, topic string, n, partitions int, sleep time.Duration, size int) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- InvokeE2EAPIContext(
			ctx,
			"/kafka/pump-topic?topic=%s&sleep=%v&n=%d&size=%d&partitions=%d",
			topic,
			sleep,
			n,
			size,
			partitions,
		)
	}()
	return done
}

func kafkaBrokerRuntime() (podName string, readinessCommand []string, err error) {
	output, err := Exec(
		"kubectl",
		"-n",
		Namespace,
		"get",
		"pod",
		"redpanda-0",
		"kafka-0",
		"--ignore-not-found",
		"-o",
		"name",
	)
	if err != nil {
		return "", nil, fmt.Errorf("find Kafka broker pod: %w, output: %s", err, output)
	}
	switch {
	case strings.Contains(output, "pod/redpanda-0"):
		return "redpanda-0", []string{"rpk", "topic", "list"}, nil
	case strings.Contains(output, "pod/kafka-0"):
		return "kafka-0", []string{"/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:9092", "--list"}, nil
	default:
		return "", nil, fmt.Errorf("neither Redpanda nor Apache Kafka broker pod was found")
	}
}

func signalKafkaBroker(signal string) error {
	podName, _, err := kafkaBrokerRuntime()
	if err != nil {
		return err
	}
	output, err := Exec(
		"kubectl",
		"-n",
		Namespace,
		"exec",
		podName,
		"--",
		"sh",
		"-c",
		fmt.Sprintf("kill -%s 1", signal),
	)
	if err != nil {
		return fmt.Errorf("signal Kafka broker with %s: %w, output: %s", signal, err, output)
	}
	return nil
}

func kafkaBrokerRestartCount(podName string) (int, error) {
	output, err := Exec(
		"kubectl",
		"-n",
		Namespace,
		"get",
		"pod",
		podName,
		"-o",
		"jsonpath={.status.containerStatuses[0].restartCount}",
	)
	if err != nil {
		return 0, fmt.Errorf("get Kafka broker restart count: %w, output: %s", err, output)
	}
	count, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil {
		return 0, fmt.Errorf("parse Kafka broker restart count %q: %w", output, err)
	}
	return count, nil
}

// RestartKafkaBroker terminates the broker process and waits for Kubernetes to restart it.
// The pod remains intact, so broker data mounted at pod scope is preserved.
func RestartKafkaBroker(timeout time.Duration) error {
	podName, _, err := kafkaBrokerRuntime()
	if err != nil {
		return err
	}
	initialRestartCount, err := kafkaBrokerRestartCount(podName)
	if err != nil {
		return err
	}
	if err = signalKafkaBroker("TERM"); err != nil {
		return err
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		restartCount, countErr := kafkaBrokerRestartCount(podName)
		if countErr == nil && restartCount > initialRestartCount {
			return WaitForKafkaBrokerReady(time.Until(deadline))
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timeout after %v waiting for Kafka broker process to restart", timeout)
}

// WaitForKafkaBrokerReady waits until the broker's topic command succeeds.
func WaitForKafkaBrokerReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		podName, readinessCommand, err := kafkaBrokerRuntime()
		if err != nil {
			lastErr = err
			time.Sleep(time.Second)
			continue
		}
		args := []string{"-n", Namespace, "exec", podName, "--"}
		args = append(args, readinessCommand...)
		output, err := Exec(
			"kubectl",
			args...,
		)
		if err == nil {
			return nil
		}
		lastErr = fmt.Errorf("%w, output: %s", err, output)
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timeout after %v waiting for Kafka broker: %w", timeout, lastErr)
}

func ExpectKafkaTopicCount(topic string, total int, timeout time.Duration) {
	log.Printf("expecting %d messages to be sunk to topic %s within %v\n", total, topic, timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		select {
		case <-ctx.Done():
			panic(fmt.Errorf("timeout waiting for %d messages in topic %q", total, topic))
		default:
			count := GetKafkaCount(topic, total)
			remaining := total - count
			log.Printf("count of Kafka topic %q is %d, %d remaining\n", topic, count, remaining)
			if count == total {
				return
			}
			if count > total {
				panic(fmt.Errorf("too many messages %d > %d", count, total))
			}
			time.Sleep(time.Second)
		}
	}
}

func GetKafkaCount(topic string, count int) int {
	count, err := strconv.Atoi(InvokeE2EAPI("/kafka/count-topic?topic=%s&count=%d", topic, count))
	fmt.Println("count", count)
	if err != nil {
		panic(fmt.Errorf("failed to count topic %q: %w", topic, err))
	}
	return count
}

func SendMessage(topic string, key string, message string, partition int) {
	InvokeE2EAPIPOST("/kafka/produce-topic?topic=%s&key=%s&partition=%d", message, topic, key, partition)
}
