/*
Copyright 2026 The Numaproj Authors.

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

const pulsarBrokerPod = "pulsar-0"

func GeneratePulsarTopicName() string {
	return fmt.Sprintf("persistent://public/default/e2e-topic-%s", rand.String(5))
}

func GeneratePulsarSubscriptionName() string {
	return fmt.Sprintf("e2e-subscription-%s", rand.String(5))
}

func SendPulsarMessage(topic, message string) {
	InvokeE2EAPIPOST("/pulsar/produce-topic?topic=%s", message, topic)
}

func PumpPulsarTopic(topic string, n int, sleep time.Duration, prefix string, size int) {
	log.Printf("pumping Pulsar topic %q sleeping %v with %d messages sized %d\n", topic, sleep, n, size)
	InvokeE2EAPI(
		"/pulsar/pump-topic?topic=%s&sleep=%v&n=%d&prefix=%s&size=%d",
		topic,
		sleep,
		n,
		prefix,
		size,
	)
}

func PumpPulsarTopicAsync(ctx context.Context, topic string, n int, sleep time.Duration, prefix string, size int) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		done <- InvokeE2EAPIContext(
			ctx,
			"/pulsar/pump-topic?topic=%s&sleep=%v&n=%d&prefix=%s&size=%d",
			topic,
			sleep,
			n,
			prefix,
			size,
		)
	}()
	return done
}

func ResetPulsarClients() {
	InvokeE2EAPI("/pulsar/reset")
}

func pulsarBrokerRestartCount() (int, error) {
	output, err := Exec(
		"kubectl",
		"-n",
		Namespace,
		"get",
		"pod",
		pulsarBrokerPod,
		"-o",
		"jsonpath={.status.containerStatuses[0].restartCount}",
	)
	if err != nil {
		return 0, fmt.Errorf("get Pulsar broker restart count: %w, output: %s", err, output)
	}
	count, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil {
		return 0, fmt.Errorf("parse Pulsar broker restart count %q: %w", output, err)
	}
	return count, nil
}

// RestartPulsarBroker terminates the standalone broker and waits for Kubernetes to restart it.
// The StatefulSet pod remains intact, so the pod-scoped broker data is preserved.
func RestartPulsarBroker(timeout time.Duration) error {
	initialRestartCount, err := pulsarBrokerRestartCount()
	if err != nil {
		return err
	}
	output, err := Exec(
		"kubectl",
		"-n",
		Namespace,
		"exec",
		pulsarBrokerPod,
		"--",
		"sh",
		"-c",
		"kill -TERM 1",
	)
	if err != nil {
		return fmt.Errorf("terminate Pulsar broker: %w, output: %s", err, output)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		restartCount, countErr := pulsarBrokerRestartCount()
		if countErr == nil && restartCount > initialRestartCount {
			return WaitForPulsarBrokerReady(time.Until(deadline))
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timeout after %v waiting for Pulsar broker process to restart", timeout)
}

// WaitForPulsarBrokerReady waits for Pulsar's end-to-end broker health check.
func WaitForPulsarBrokerReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		output, err := Exec(
			"kubectl",
			"-n",
			Namespace,
			"exec",
			pulsarBrokerPod,
			"--",
			"curl",
			"--fail",
			"--silent",
			"--show-error",
			"http://127.0.0.1:8080/admin/v2/brokers/health",
		)
		if err == nil {
			return nil
		}
		lastErr = fmt.Errorf("%w, output: %s", err, output)
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timeout after %v waiting for Pulsar broker: %w", timeout, lastErr)
}
