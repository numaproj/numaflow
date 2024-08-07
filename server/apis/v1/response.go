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

import (
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
)

var (
	jsonMarshaller = new(runtime.JSONPb)
)

type NumaflowAPIResponse struct {
	// ErrMsg provides more detailed error information. If API call succeeds, the ErrMsg is nil.
	ErrMsg *string `json:"errMsg,omitempty"`
	// Data is the response body.
	Data any `json:"data"`
}

// NewNumaflowAPIResponse creates a new NumaflowAPIResponse.
func NewNumaflowAPIResponse(errMsg *string, data any) NumaflowAPIResponse {
	return NumaflowAPIResponse{
		ErrMsg: errMsg,
		Data:   data,
	}
}

// MarshalJSON implements json.Marshaler.
// It will marshal the response into a JSON object without having nested "value" field in the JSON for google.protobuf.* types.
func (r NumaflowAPIResponse) MarshalJSON() ([]byte, error) {
	m := map[string]any{
		"errMsg": r.ErrMsg,
		"data":   r.Data,
	}
	return jsonMarshaller.Marshal(m)
}
