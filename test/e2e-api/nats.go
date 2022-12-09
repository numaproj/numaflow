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
	"time"

	"github.com/nats-io/nats.go"
	natslib "github.com/nats-io/nats.go"
)

func init() {
	url := "nats"
	testingToken := "testingtoken"

	http.HandleFunc("/nats/pump-subject", func(w http.ResponseWriter, r *http.Request) {
		subject := r.URL.Query().Get("subject")
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

		opts := []nats.Option{natslib.Token(testingToken)}
		nc, err := natslib.Connect(url, opts...)
		if err != nil {
			log.Println(err)
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		defer nc.Close()

		start := time.Now()
		_, _ = fmt.Fprintf(w, "sending %d messages of size %d to %q\n", n, mf.size, subject)

		for i := 0; i < n || n < 0; i++ {
			select {
			case <-r.Context().Done():
				return
			default:
				err := nc.Publish(subject, []byte(mf.newMessage(i)))
				if err != nil {
					_, _ = fmt.Fprintf(w, "ERROR: %v\n", err)
				}
				time.Sleep(duration)
			}
		}
		_, _ = fmt.Fprintf(w, "sent %d messages of size %d at %.0f TPS to %q\n", n, mf.size, float64(n)/time.Since(start).Seconds(), subject)
	})
}
