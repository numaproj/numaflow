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
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var redisClient *redis.Client

func init() {

	// When we use this API to validate e2e test result, we always assume a redis UDSink is used
	// to persist data to a redis instance listening on port 6379.
	redisClient = redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	// get-msg-count-contains returns no. of occurrences of the targetStr in redis that are written by pipelineName and sinkName.
	http.HandleFunc("/redis/get-msg-count-contains", func(w http.ResponseWriter, r *http.Request) {
		pipelineName := r.URL.Query().Get("pipelineName")
		sinkName := r.URL.Query().Get("sinkName")
		targetStr, err := url.QueryUnescape(r.URL.Query().Get("targetStr"))
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		count, err := redisClient.HGet(context.Background(), fmt.Sprintf("%s:%s", pipelineName, sinkName), targetStr).Result()

		if err != nil {
			log.Println(err)
			// If targetStr doesn't exist in the hash, HGet returns an error, meaning count is 0.
			w.WriteHeader(200)
			_, _ = w.Write([]byte("0"))
			return
		}

		w.WriteHeader(200)
		_, _ = w.Write([]byte(count))
	})

	http.HandleFunc("/redis/pump-stream", func(w http.ResponseWriter, r *http.Request) {
		stream := r.URL.Query().Get("stream")
		keysValuesJsonEncoded := r.URL.Query().Get("keysvalues")
		keysValuesJson, err := url.QueryUnescape(keysValuesJsonEncoded)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var keysValues map[string]string
		err = json.Unmarshal([]byte(keysValuesJson), &keysValues)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		valueMap := make(map[string]interface{})
		for k, v := range keysValues {
			valueMap[k] = interface{}(v)
		}

		size, err := strconv.Atoi(r.URL.Query().Get("size"))
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		duration, err := time.ParseDuration(r.URL.Query().Get("sleep"))
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		ns := r.URL.Query().Get("n")
		if ns == "" {
			ns = "-1"
		}
		n, err := strconv.Atoi(ns)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)

		start := time.Now()
		_, _ = fmt.Fprintf(w, "sending %d messages of size %d to %q\n", n, size, stream)

		for i := 0; i < n || n < 0; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
				result := redisClient.XAdd(r.Context(), &redis.XAddArgs{Stream: stream, Values: valueMap})
				if result.Err() != nil {
					http.Error(w, result.Err().Error(), http.StatusFailedDependency)
					return
				}
				time.Sleep(duration)
			}
		}
		_, _ = fmt.Fprintf(w, "sent %d messages of size %d at %.0f TPS to %q\n", n, size, float64(n)/time.Since(start).Seconds(), stream)
	})
}
