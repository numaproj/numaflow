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
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"k8s.io/apimachinery/pkg/util/wait"
	"net/http"
	"time"
)

func init() {
	http.HandleFunc("/http/send-message", func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()
		pName := r.URL.Query().Get("pipeline")
		vName := r.URL.Query().Get("vertex")

		buf, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			panic(err)
		}

		httpClient := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}

		// Posting right after vertex creation sometimes gets the "dial tcp: connect: connection refused" error.
		// Adding retry to mitigate such issue.
		// 3 attempts with 2 second fixed wait time are tested sufficient for it.
		var retryBackOff = wait.Backoff{
			Factor:   1,
			Jitter:   0,
			Steps:    3,
			Duration: time.Second * 2,
		}

		_ = wait.ExponentialBackoffWithContext(ctx, retryBackOff, func() (done bool, err error) {
			_, err = httpClient.Post(fmt.Sprintf("https://%s-%s:8443/vertices/%s", pName, vName, vName), "application/json", bytes.NewBuffer(buf))
			if err == nil {
				return true, nil
			}
			fmt.Printf("Got error %v, retrying.\n", err)
			return false, nil
		})

		if err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(err.Error()))
			panic(err)
		}
	})
}
