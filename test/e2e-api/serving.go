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
	"fmt"
	"io"
	"log"
	"net/http"
)

type ServingController struct {
	client *http.Client
}

func NewServingController() *ServingController {
	return &ServingController{
		client: &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
	}
}

func (h *ServingController) SendMessage(w http.ResponseWriter, r *http.Request) {
	host := r.URL.Query().Get("host")
	sync := r.URL.Query().Get("sync")
	reqBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	uri := fmt.Sprintf("https://%s:8443/v1/process/sync", host)
	if sync == "false" {
		uri = fmt.Sprintf("https://%s:8443/v1/process/async", host)
	}

	postReq, err := http.NewRequest("POST", uri, bytes.NewBuffer(reqBytes))
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp, err := h.client.Do(postReq)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("[ERROR] Expected %d, Got %d", http.StatusOK, resp.StatusCode)
		http.Error(w, fmt.Sprintf("Bad status: %s", resp.Status), http.StatusInternalServerError)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[ERROR] Reading response body from Serving source: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(body)
}

// Close closes the http client
func (h *ServingController) Close() {
	h.client.CloseIdleConnections()
}
