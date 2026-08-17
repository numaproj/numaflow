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
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const pulsarWebSocketURL = "ws://pulsar:8080/ws/v2/producer"

type PulsarController struct {
	lock      sync.Mutex
	producers map[string]*pulsarProducer
}

type pulsarProducer struct {
	lock    sync.Mutex
	conn    *websocket.Conn
	context uint64
}

type pulsarProducerRequest struct {
	Payload string `json:"payload"`
	Context string `json:"context"`
}

type pulsarProducerResponse struct {
	Result   string `json:"result"`
	ErrorMsg string `json:"errorMsg"`
	Context  string `json:"context"`
}

func NewPulsarController() *PulsarController {
	return &PulsarController{producers: make(map[string]*pulsarProducer)}
}

func pulsarProducerURL(topic string) (string, error) {
	topicPath, found := strings.CutPrefix(topic, "persistent://")
	if !found {
		return "", fmt.Errorf("Pulsar topic %q must use the persistent scheme", topic)
	}
	parts := strings.Split(topicPath, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", fmt.Errorf("Pulsar topic %q must contain tenant, namespace, and topic", topic)
	}
	return fmt.Sprintf(
		"%s/persistent/%s/%s/%s",
		pulsarWebSocketURL,
		url.PathEscape(parts[0]),
		url.PathEscape(parts[1]),
		url.PathEscape(parts[2]),
	), nil
}

func (p *PulsarController) producer(topic string) (*pulsarProducer, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if producer := p.producers[topic]; producer != nil {
		return producer, nil
	}

	producerURL, err := pulsarProducerURL(topic)
	if err != nil {
		return nil, err
	}
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 5 * time.Second
	conn, _, err := dialer.Dial(producerURL, nil)
	if err != nil {
		return nil, fmt.Errorf("connect Pulsar WebSocket producer for %q: %w", topic, err)
	}
	producer := &pulsarProducer{conn: conn}
	p.producers[topic] = producer
	return producer, nil
}

func (p *PulsarController) send(topic string, payload []byte) error {
	producer, err := p.producer(topic)
	if err != nil {
		return err
	}
	if err = producer.send(payload); err != nil {
		p.lock.Lock()
		if p.producers[topic] == producer {
			delete(p.producers, topic)
			_ = producer.close()
		}
		p.lock.Unlock()
		return fmt.Errorf("publish Pulsar message to %q: %w", topic, err)
	}
	return nil
}

func (p *pulsarProducer) send(payload []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.context++
	context := strconv.FormatUint(p.context, 10)
	deadline := time.Now().Add(10 * time.Second)
	if err := p.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := p.conn.WriteJSON(pulsarProducerRequest{
		Payload: base64.StdEncoding.EncodeToString(payload),
		Context: context,
	}); err != nil {
		return err
	}
	if err := p.conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	var response pulsarProducerResponse
	if err := p.conn.ReadJSON(&response); err != nil {
		return err
	}
	if response.Context != context {
		return fmt.Errorf("unexpected response context %q, expected %q", response.Context, context)
	}
	if response.Result != "ok" {
		return fmt.Errorf("%s: %s", response.Result, response.ErrorMsg)
	}
	return nil
}

func (p *pulsarProducer) close() error {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.conn.Close()
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
	if err = p.send(topic, payload); err != nil {
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
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	started := time.Now()
	_, _ = fmt.Fprintf(w, "sending %d messages of size %d to %q\n", count, messageFactory.size, topic)
	for i := 0; i < count; i++ {
		if err = p.send(topic, []byte(messageFactory.newMessage(i))); err != nil {
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

func (p *PulsarController) ResetHandler(w http.ResponseWriter, _ *http.Request) {
	p.Close()
	w.WriteHeader(http.StatusNoContent)
}

func (p *PulsarController) Close() {
	p.lock.Lock()
	defer p.lock.Unlock()

	for topic, producer := range p.producers {
		_ = producer.close()
		delete(p.producers, topic)
	}
}
