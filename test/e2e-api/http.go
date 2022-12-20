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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/numaproj/numaflow/test/fixtures"
	"io"
	"net/http"
)

var httpClient *http.Client

func init() {
	httpClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	// send-message API is used to post data to a http source vertex pod.
	// The API takes in two parameters(podIp and vertexName) and constructs the target url as
	// https://{podIp}:8443/vertices/{vertexName}.
	http.HandleFunc("/http/send-message", func(w http.ResponseWriter, r *http.Request) {
		podIp := r.URL.Query().Get("podIp")
		vertexName := r.URL.Query().Get("vertexName")
		reqBytes, err := io.ReadAll(r.Body)
		checkError(err, w)

		var req fixtures.HttpPostRequest
		err = json.Unmarshal(reqBytes, &req)
		checkError(err, w)

		postReq, err := http.NewRequest("POST", fmt.Sprintf("https://%s:8443/vertices/%s", podIp, vertexName), bytes.NewBuffer(req.Body))
		checkError(err, w)

		for k, v := range req.Header {
			postReq.Header.Add(k, v)
		}
		_, err = httpClient.Do(postReq)
		checkError(err, w)
	})
}

func checkError(err error, w http.ResponseWriter) {
	if err != nil {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(err.Error()))
	}
}
