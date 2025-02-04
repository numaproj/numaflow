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
	"fmt"
	"log"
	"time"
)

func PumpPulsarTopic(topic string, n int, opts ...interface{}) {
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
	log.Printf("pumping Pulsar topic %q sleeping %v with %d messages sized %d\n", topic, sleep, n, size)
	InvokeE2EAPI("/pulsar/pump-topic?topic=%s&sleep=%v&n=%d&prefix=%s&size=%d", topic, sleep, n, prefix, size)
}
