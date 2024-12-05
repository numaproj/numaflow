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
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

const broker = "pulsar://pulsar-service:6650"
const jwtAuthToken = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.fDSXQNpGAYCxjsuBVsH4S3eK9YYtzpz8_vAYqLpTp2o"

type PulsarController struct {
	client   pulsar.Client
	producer pulsar.Producer
	lock     sync.Mutex
}

func NewPulsarController() *PulsarController {
	return new(PulsarController)
}

func (p *PulsarController) getProducer(topic string) pulsar.Producer {
	if p.producer != nil {
		return p.producer
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            broker,
		Authentication: pulsar.NewAuthenticationToken(jwtAuthToken),
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}
	p.client = client

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		log.Fatalf("Could not instantiate Pulsar producer: %v", err)
	}
	p.producer = producer
	return producer
}

func (p *PulsarController) PumpTopicHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
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

	producer := p.getProducer(topic)

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(200)

	start := time.Now()
	_, _ = fmt.Fprintf(w, "sending %d messages of size %d to %q\n", n, mf.size, topic)

	for i := 0; i < n || n < 0; i++ {
		select {
		case <-ctx.Done():
			return
		default:
			_, err := producer.Send(ctx, &pulsar.ProducerMessage{
				Payload: []byte(mf.newMessage(i)),
			})
			if err != nil {
				_, _ = fmt.Fprintf(w, "ERROR: %v\n", err)
			}
			time.Sleep(duration)
		}
	}
	_, _ = fmt.Fprintf(w, "sent %d messages of size %d at %.0f TPS to %q\n", n, mf.size, float64(n)/time.Since(start).Seconds(), topic)
}

func (p *PulsarController) Close() {
	if p.client == nil {
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	p.producer.Close()
	p.client.Close()
}
