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

package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"log"
	"net/http"
	"net/url"
)

func init() {
	// get-msg-count-contains takes a targetRegex and returns number of keys in redis
	// which contain a substring matching the targetRegex.
	http.HandleFunc("/redis/get-msg-count-contains", func(w http.ResponseWriter, r *http.Request) {
		targetRegex, err := url.QueryUnescape(r.URL.Query().Get("targetRegex"))

		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		// When we use this API to validate e2e test result, we always assume a redis UDSink is used
		// to persist data to a redis instance listening on port 6379.
		client := redis.NewClient(&redis.Options{
			Addr: "redis:6379",
		})

		// Redis Keys API uses scan to retrieve data, which is not best practice in terms of performance.
		// TODO - Look into replacing it with a more efficient API or data structure.
		keyList, err := client.Keys(context.Background(), fmt.Sprintf("*%s*", targetRegex)).Result()
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		w.WriteHeader(200)
		_, _ = w.Write([]byte(fmt.Sprint(len(keyList))))
	})
}
