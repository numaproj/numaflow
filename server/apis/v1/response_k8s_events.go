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

package v1

import "fmt"

type K8sEventsResponse struct {
	TimeStamp int64  `json:"timestamp"`
	Type      string `json:"type"`
	Object    string `json:"object"`
	Reason    string `json:"reason"`
	Message   string `json:"message"`
}

// NewK8sEventsResponse creates a new K8sEventsResponse object with the given inputs.
func NewK8sEventsResponse(timestamp int64, eventType, objectKind, objectName, reason, message string) K8sEventsResponse {

	return K8sEventsResponse{
		TimeStamp: timestamp,
		Type:      eventType,
		Object:    fmt.Sprintf("%s/%s", objectKind, objectName),
		Reason:    reason,
		Message:   message,
	}
}
