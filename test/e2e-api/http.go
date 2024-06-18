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
	"io"
	"log"
	"net/http"

	"github.com/numaproj/numaflow/test/fixtures"
)

type HttpController struct {
	client *http.Client
}

func initNewHttp() *HttpController {
	return &HttpController{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	}
}

func NewHttpController() *HttpController {
	return &HttpController{
		client: nil,
	}
}

func (h *HttpController) SendMessage(w http.ResponseWriter, r *http.Request) {

	m.Lock()
	if h.client == nil {
		h = initNewHttp()
	}
	m.Unlock()

	host := r.URL.Query().Get("host")
	vertexName := r.URL.Query().Get("vertexName")
	reqBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req fixtures.HttpPostRequest
	err = json.Unmarshal(reqBytes, &req)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	postReq, err := http.NewRequest("POST", fmt.Sprintf("https://%s:8443/vertices/%s", host, vertexName), bytes.NewBuffer(req.Body))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for k, v := range req.Header {
		postReq.Header.Add(k, v)
	}
	_, err = h.client.Do(postReq)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Close closes the http client
func (h *HttpController) Close() {
	h.client.CloseIdleConnections()
}
