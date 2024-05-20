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
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const bootstrapServers = "kafka-broker:9092"

type KafkaController struct {
	brokers     []string
	adminClient sarama.ClusterAdmin
	producer    sarama.SyncProducer
	consumer    sarama.Consumer
}

func NewKafkaController() *KafkaController {
	// initialize Kafka handlers
	var brokers = []string{bootstrapServers}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// if partition is specified, use manual partitioner
	config.Producer.Partitioner = sarama.NewManualPartitioner

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %v", err)
	}

	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka consumer: %v", err)
	}

	adminClient, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka admin client: %v", err)
	}
	return &KafkaController{
		brokers:     brokers,
		adminClient: adminClient,
		producer:    producer,
		consumer:    consumer,
	}
}

func (kh *KafkaController) CreateTopicHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	partitions, err := strconv.Atoi(r.URL.Query().Get("partitions"))
	if err != nil {
		log.Println(err)
		http.Error(w, "Invalid number of partitions", http.StatusBadRequest)
		return
	}
	if err = kh.adminClient.CreateTopic(topic, &sarama.TopicDetail{NumPartitions: int32(partitions), ReplicationFactor: 1}, true); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(201)
}

func (kh *KafkaController) DeleteTopicHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if err := kh.adminClient.DeleteTopic(topic); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(201)
}

func (kh *KafkaController) ListTopicsHandler(w http.ResponseWriter, r *http.Request) {
	topics, err := kh.adminClient.ListTopics()
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(200)
	for topic, details := range topics {
		_, _ = fmt.Fprintf(w, "Topic: %s, Partitions: %d\n", topic, details.NumPartitions)
	}
}

func (kh *KafkaController) CountTopicHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	count, err := strconv.Atoi(r.URL.Query().Get("count"))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	partitions, err := kh.consumer.Partitions(topic)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msgCount := 0
	msgs := make(chan *sarama.ConsumerMessage, 256)
	doneCh := make(chan struct{})
	errCh := make(chan error)
	var wg sync.WaitGroup

	var consumers []sarama.PartitionConsumer

	for _, partition := range partitions {
		pConsumer, err := kh.consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		consumers = append(consumers, pConsumer)
		wg.Add(1)
		log.Println("Starting consumer for partition: ", partition)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for {
				select {
				case <-doneCh:
					return
				case msg := <-pc.Messages():
					select {
					case msgs <- msg:
					case <-doneCh:
						return
					}
				case consumerError := <-pc.Errors():
					errCh <- consumerError.Err
					return
				}
			}
		}(pConsumer)
	}

readLoop:
	for msgCount < count {
		select {
		case msg := <-msgs:
			msgCount++
			log.Println("Received messages: ", string(msg.Key), string(msg.Value), msg.Offset, msgCount, " partition - ", msg.Partition)
		case consumerError := <-errCh:
			log.Println("Received consumerError - ", consumerError.Error())
			break readLoop
		}
	}

	close(doneCh)

	for _, c := range consumers {
		_ = c.Close()
	}

	w.WriteHeader(200)
	_, _ = w.Write([]byte(fmt.Sprint(count)))
}

func (kh *KafkaController) ProduceTopicHandler(w http.ResponseWriter, r *http.Request) {
	var (
		partition int
		err       error
	)
	topic := r.URL.Query().Get("topic")
	key := r.URL.Query().Get("key")
	queryPartition := r.URL.Query().Get("partition")

	if queryPartition == "" {
		partition = 0
	} else {
		partition, err = strconv.Atoi(queryPartition)
		if err != nil {
			log.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	buf, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	message := &sarama.ProducerMessage{
		Topic:     topic,
		Value:     sarama.ByteEncoder(buf),
		Key:       sarama.ByteEncoder(key),
		Partition: int32(partition),
	}
	p, of, err := kh.producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to produce message to topic %s: %s\n", topic, err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	// send the partition and offset as response
	w.WriteHeader(200)
	_, _ = w.Write([]byte(fmt.Sprintf("Partition - %d: Offset - %d", p, of)))
}

func (kh *KafkaController) PumpTopicHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	mf := newMessageFactory(r.URL.Query())
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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(200)

	start := time.Now()
	_, _ = fmt.Fprintf(w, "sending %d messages of size %d to %q\n", n, mf.size, topic)

	for i := 0; i < n || n < 0; i++ {
		select {
		case <-r.Context().Done():
			return
		default:
			message := &sarama.ProducerMessage{
				Topic:     topic,
				Value:     sarama.ByteEncoder(mf.newMessage(i)),
				Key:       sarama.ByteEncoder(strconv.Itoa(i)),
				Partition: int32(0),
			}
			_, _, err := kh.producer.SendMessage(message)
			if err != nil {
				_, _ = fmt.Fprintf(w, "ERROR: %v\n", err)
			}
			time.Sleep(duration)
		}
	}
	_, _ = fmt.Fprintf(w, "sent %d messages of size %d at %.0f TPS to %q\n", n, mf.size, float64(n)/time.Since(start).Seconds(), topic)
}

func (kh *KafkaController) Close() {
	_ = kh.producer.Close()
	_ = kh.consumer.Close()
	_ = kh.adminClient.Close()
}
