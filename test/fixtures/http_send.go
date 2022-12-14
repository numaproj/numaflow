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
	"log"
)

// SendMessageTo sends msg to a http source vertex.
func SendMessageTo(pipelineName string, vertexName string, msg []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
		}
	}()
	log.Printf("Sending msg %v to pipeline %s, vertex %s\n", msg, pipelineName, vertexName)
	InvokeE2EAPIPOST("/http/send-message?pipeline=%s&vertex=%s", string(msg[:]), pipelineName, vertexName)
	return
}
