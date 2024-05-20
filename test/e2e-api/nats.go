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
	"strconv"
	"time"

	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NatsController struct {
	client *natslib.Conn
}

func NewNatsController(url string, token string) *NatsController {
	opts := []natslib.Option{natslib.Token(token)}
	nc, err := natslib.Connect(url, opts...)
	if err != nil {
		log.Fatal(err)
	}
	return &NatsController{
		client: nc,
	}
}

func (n *NatsController) PumpSubject(w http.ResponseWriter, r *http.Request) {
	subject := r.URL.Query().Get("subject")
	msg := r.URL.Query().Get("msg")
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
	nCount, err := strconv.Atoi(ns)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(200)

	start := time.Now()
	_, _ = fmt.Fprintf(w, "sending %d messages of size %d to %q\n", nCount, size, subject)

	for i := 0; i < nCount || nCount < 0; i++ {
		select {
		case <-r.Context().Done():
			return
		default:
			err := n.client.Publish(subject, []byte(msg))
			if err != nil {
				_, _ = fmt.Fprintf(w, "ERROR: %v\n", err)
			}
			time.Sleep(duration)
		}
	}
	_, _ = fmt.Fprintf(w, "sent %d messages of size %d at %.0f TPS to %q\n", nCount, size, float64(nCount)/time.Since(start).Seconds(), subject)
}

func (n *NatsController) PumpJetstream(w http.ResponseWriter, r *http.Request) {
	streamName := r.URL.Query().Get("stream")
	msg := r.URL.Query().Get("msg")

	ns := r.URL.Query().Get("n")
	if ns == "" {
		ns = "-1"
	}
	nCount, err := strconv.Atoi(ns)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	js, err := jetstream.New(n.client)
	if err != nil {
		log.Println("Creating jetstream instance:", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"jse2e.events.>"},
	})
	if err != nil {
		log.Printf("Creating stream %q: %v", streamName, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(200)

	start := time.Now()
	_, _ = fmt.Fprintf(w, "sending %d messages to %q\n", nCount, stream)

	for i := 0; i < nCount || nCount < 0; i++ {
		select {
		case <-r.Context().Done():
			return
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := js.Publish(ctx, "jse2e.events.debug", []byte(msg))
			cancel()
			if err != nil {
				log.Printf("Publishing events to jetstream %q: %v", streamName, err)
				_, _ = fmt.Fprintf(w, "ERROR: %v\n", err)
			}
		}
	}
	_, _ = fmt.Fprintf(w, "sent %d messages at %.0f TPS to %q\n", nCount, float64(nCount)/time.Since(start).Seconds(), streamName)
}

func (n *NatsController) Close() {
	n.client.Close()
}
