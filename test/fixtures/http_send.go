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

package fixtures

import (
	"encoding/json"
	"fmt"
)

type HttpPostRequest struct {
	Header map[string]string `json:"header"`
	Body   []byte            `json:"body"`
}

// NewHttpPostRequest constructor for HttpPostRequest
func NewHttpPostRequest() HttpPostRequest {
	return HttpPostRequest{}
}

func (b HttpPostRequest) WithHeader(k, v string) HttpPostRequest {
	if b.Header == nil {
		b.Header = map[string]string{}
	}
	b.Header[k] = v
	return b
}

func (b HttpPostRequest) WithBody(body []byte) HttpPostRequest {
	b.Body = body
	return b
}

// SendMessageTo sends a http post request to a pod in http source vertex.
func SendMessageTo(podIp string, vertexName string, r HttpPostRequest) {
	req, err := json.Marshal(r)
	if err != nil {
		panic(fmt.Sprintf("Failed serializing the request %v into json format. %v", r, err))
	}
	InvokeE2EAPIPOST("/http/send-message?podIp=%s&vertexName=%s", string(req[:]), podIp, vertexName)
}
