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
	"net/http"
)

func main() {

	// initialize kafka handlers
	kafkaController := NewKafkaController()
	http.HandleFunc("/kafka/create-topic", kafkaController.CreateTopicHandler)
	http.HandleFunc("/kafka/delete-topic", kafkaController.DeleteTopicHandler)
	http.HandleFunc("/kafka/list-topics", kafkaController.ListTopicsHandler)
	http.HandleFunc("/kafka/count-topic", kafkaController.CountTopicHandler)
	http.HandleFunc("/kafka/produce-topic", kafkaController.ProduceTopicHandler)
	http.HandleFunc("/kafka/pump-topic", kafkaController.PumpTopicHandler)

	// initialize Redis handlers
	redisController := NewRedisController()
	http.HandleFunc("/redis/get-msg-count-contains", redisController.GetMsgCountContains)
	http.HandleFunc("/redis/pump-stream", redisController.PumpStream)

	// initialize http handlers
	httpController := NewHttpController()
	http.HandleFunc("/http/send-message", httpController.SendMessage)

	// initialize NATS handler
	natsController := NewNatsController("nats", "testingtoken")
	http.HandleFunc("/nats/pump-subject", natsController.PumpSubject)

	if err := http.ListenAndServe(":8378", nil); err != nil {
		panic(err)
	}

	// close all controllers
	kafkaController.Close()
	redisController.Close()
	httpController.Close()
	natsController.Close()
}
