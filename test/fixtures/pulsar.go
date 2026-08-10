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
	"time"

	"k8s.io/apimachinery/pkg/util/rand"
)

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
