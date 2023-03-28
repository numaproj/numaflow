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
	"net/url"
	"strconv"
	"time"
)

// GetMsgCountContains returns number of occurrences of the targetStr in redis that are written by pipelineName, sinkName.
func GetMsgCountContains(pipelineName, sinkName, targetStr string) int {
	str := InvokeE2EAPI("/redis/get-msg-count-contains?pipelineName=%s&sinkName=%s&targetStr=%s", pipelineName, sinkName, url.QueryEscape(targetStr))
	count, err := strconv.Atoi(str)
	if err != nil {
		panic(fmt.Sprintf("Can't parse string %s to an integer.", str))
	}
	return count
}

// function to invoke Redis Source
func PumpRedisStream(stream string, n int, opts ...interface{}) {
	var sleep time.Duration
	var keysValuesJson string
	var size int
	for _, opt := range opts {
		switch v := opt.(type) {
		case time.Duration:
			sleep = v
		case string:
			keysValuesJson = v
		case int:
			size = v
		default:
			panic(fmt.Errorf("unexpected option type %T", opt))
		}
	}
	keysValuesJsonEncoded := url.QueryEscape(keysValuesJson)
	log.Printf("Pumping Redis stream %q sleeping %v with %d messages sized %d, keys/values=%q\n", stream, sleep, n, size, keysValuesJson)
	InvokeE2EAPI("/redis/pump-stream?stream=%s&sleep=%v&n=%d&keysvalues=%s&size=%d", stream, sleep, n, keysValuesJsonEncoded, size)

}
