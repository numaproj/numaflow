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
	"time"

	"k8s.io/apimachinery/pkg/util/rand"
)

func CreateKafkaTopic() string {
	topic := fmt.Sprintf("e2e-topic-%s", rand.String(5))
	log.Printf("create Kafka topic %q\n", topic)
	InvokeE2EAPI("/kafka/create-topic?topic=%s", topic)
	return topic
}

func DeleteKafkaTopic(topic string) string {
	log.Printf("delete Kafka topic %q\n", topic)
	InvokeE2EAPI("/kafka/delete-topic?topic=%s", topic)
	return topic
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

func SendMessage(topic string, key string, message string) {
	InvokeE2EAPIPOST("/kafka/produce-topic?topic=%s&key=%s", message, topic, key)

}

func ValidateMessage(topic string, key string, position int) {

}
