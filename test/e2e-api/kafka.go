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
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

func init() {
	const bootstrapServers = "kafka-broker:9092"
	var brokers = []string{bootstrapServers}
	http.HandleFunc("/kafka/create-topic", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		defer admin.Close()
		if err = admin.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, true); err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(201)
	})

	http.HandleFunc("/kafka/delete-topic", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		defer admin.Close()
		if err = admin.DeleteTopic(topic); err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(201)
	})
	http.HandleFunc("/kafka/list-topics", func(w http.ResponseWriter, r *http.Request) {
		consumer, err := sarama.NewConsumer(brokers, sarama.NewConfig())
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		defer consumer.Close()
		topics, err := consumer.Topics()
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(200)
		_, _ = fmt.Fprintf(w, "Total topics : %s", topics)
	})

	http.HandleFunc("/kafka/count-topic", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		count, err := strconv.Atoi(r.URL.Query().Get("count"))
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		consumer, err := sarama.NewConsumer(brokers, sarama.NewConfig())
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		defer consumer.Close()
		pConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		msgCount := 0
		for msgCount < count {
			select {
			case msg := <-pConsumer.Messages():
				msgCount++
				log.Println("Received messages: ", string(msg.Key), string(msg.Value), msg.Offset, msgCount)
			case consumerError := <-pConsumer.Errors():
				log.Println("Received consumerError.", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
			}
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(fmt.Sprint(count)))
	})

	http.HandleFunc("/kafka/produce-topic", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		key := r.URL.Query().Get("key")
		buf, err := io.ReadAll(r.Body)
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true

		syncProducer, err := sarama.NewSyncProducer(brokers, config)

		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		defer syncProducer.Close()
		message := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(buf),
			Key:   sarama.ByteEncoder([]byte(key)),
		}
		if _, _, err := syncProducer.SendMessage(message); err != nil {
			_, _ = fmt.Fprintf(w, "ERROR: %v\n", err)
		}
	})

	http.HandleFunc("/kafka/pump-topic", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		mf := newMessageFactory(r.URL.Query())
		duration, err := time.ParseDuration(r.URL.Query().Get("sleep"))
		if err != nil {
			log.Println(err)
			w.WriteHeader(400)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		ns := r.URL.Query().Get("n")
		if ns == "" {
			ns = "-1"
		}
		n, err := strconv.Atoi(ns)
		if err != nil {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(err.Error()))
			return
		}

		w.Header().Set("Content-Type", "application/octet-stream")
		w.WriteHeader(200)

		config := sarama.NewConfig()
		config.Producer.Return.Successes = true

		syncProducer, err := sarama.NewSyncProducer(brokers, config)

		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		defer syncProducer.Close()

		start := time.Now()
		_, _ = fmt.Fprintf(w, "sending %d messages of size %d to %q\n", n, mf.size, topic)

		for i := 0; i < n || n < 0; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
				message := &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.ByteEncoder(mf.newMessage(i)),
					Key:   sarama.ByteEncoder([]byte(strconv.Itoa(i))),
				}
				_, _, err := syncProducer.SendMessage(message)
				if err != nil {
					_, _ = fmt.Fprintf(w, "ERROR: %v\n", err)
				}
				time.Sleep(duration)
			}
		}
		_, _ = fmt.Fprintf(w, "sent %d messages of size %d at %.0f TPS to %q\n", n, mf.size, float64(n)/time.Since(start).Seconds(), topic)
	})
}
