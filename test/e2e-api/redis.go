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
	"log"
	"net/http"
	"net/url"
	"sync"

	"github.com/redis/go-redis/v9"
)

type RedisController struct {
	client *redis.Client
	mLock  sync.RWMutex
}

// getter method for lazy loading. creates and returns redis client only when required
func (h *RedisController) getRedisClient() *redis.Client {
	if h.client != nil {
		return h.client
	}
	h.mLock.Lock()
	defer h.mLock.Unlock()
	if h.client != nil {
		return h.client
	}
	h.client = redis.NewClient(&redis.Options{
		Addr: "redis:6379",
	})

	log.Println("new redis client created")
	return h.client
}

func NewRedisController() *RedisController {
	// When we use this API to validate e2e test result, we always assume a redis UDSink is used
	// to persist data to a redis instance listening on port 6379.
	return &RedisController{
		client: nil,
	}
}

func (h *RedisController) GetMsgCountContains(w http.ResponseWriter, r *http.Request) {

	redisClient := h.getRedisClient()

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
}

// Close closes the Redis client.
func (h *RedisController) Close() {
	h.mLock.Lock()
	defer h.mLock.Unlock()
	if h.client != nil {
		if err := h.client.Close(); err != nil {
			log.Println(err)
		}
	}

}
