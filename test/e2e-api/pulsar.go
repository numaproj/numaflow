/*
Copyright 2026 The Numaproj Authors.

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
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

const pulsarBrokerURL = "pulsar://pulsar:6650"

type PulsarController struct {
	lock      sync.Mutex
	client    pulsar.Client
	producers map[string]pulsar.Producer
}

func NewPulsarController() *PulsarController {
	return &PulsarController{producers: make(map[string]pulsar.Producer)}
}

func (p *PulsarController) producer(topic string) (pulsar.Producer, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if producer := p.producers[topic]; producer != nil {
		return producer, nil
	}

	if p.client == nil {
		client, err := pulsar.NewClient(pulsar.ClientOptions{
			URL:               pulsarBrokerURL,
			ConnectionTimeout: 5 * time.Second,
			OperationTimeout:  10 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("create Pulsar client: %w", err)
		}
		p.client = client
	}

	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	if err != nil {
		return nil, fmt.Errorf("create Pulsar producer for %q: %w", topic, err)
	}
	p.producers[topic] = producer
	return producer, nil
}

func (p *PulsarController) ProduceTopicHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("read message: %v", err), http.StatusBadRequest)
		return
	}
	producer, err := p.producer(topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if _, err = producer.Send(r.Context(), &pulsar.ProducerMessage{Payload: payload}); err != nil {
		http.Error(w, fmt.Sprintf("produce Pulsar message: %v", err), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (p *PulsarController) PumpTopicHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}
	sleep, err := time.ParseDuration(r.URL.Query().Get("sleep"))
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid sleep duration: %v", err), http.StatusBadRequest)
		return
	}
	count, err := strconv.Atoi(r.URL.Query().Get("n"))
	if err != nil || count < 0 {
		http.Error(w, "n must be a non-negative integer", http.StatusBadRequest)
		return
	}
	messageFactory := newMessageFactory(r.URL.Query())
	producer, err := p.producer(topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	started := time.Now()
	_, _ = fmt.Fprintf(w, "sending %d messages of size %d to %q\n", count, messageFactory.size, topic)
	for i := 0; i < count; i++ {
		if _, err = producer.Send(r.Context(), &pulsar.ProducerMessage{
			Payload: []byte(messageFactory.newMessage(i)),
		}); err != nil {
			_, _ = fmt.Fprintf(w, "ERROR: %v\n", err)
			return
		}
		if sleep > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(sleep):
			}
		}
	}
	_, _ = fmt.Fprintf(
		w,
		"sent %d messages of size %d at %.0f TPS to %q\n",
		count,
		messageFactory.size,
		float64(count)/time.Since(started).Seconds(),
		topic,
	)
}

func (p *PulsarController) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	for topic, producer := range p.producers {
		producer.Close()
		delete(p.producers, topic)
	}
	if p.client != nil {
		p.client.Close()
		p.client = nil
	}
}
