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

package testutils

import (
	"context"

	"github.com/numaproj/numaflow/pkg/isb"
)

// CopyUDFTestApply applies a copy UDF that simply copies the input to output.
func CopyUDFTestApply(ctx context.Context, readMessage *isb.ReadMessage) ([]*isb.Message, error) {
	_ = ctx
	offset := readMessage.ReadOffset
	payload := readMessage.Body.Payload
	parentPaneInfo := readMessage.PaneInfo

	// apply UDF
	_ = payload
	// copy the payload
	result := payload
	var key string

	writeMessage := &isb.Message{
		Header: isb.Header{
			PaneInfo: parentPaneInfo,
			ID:       offset.String(),
			Key:      key,
		},
		Body: isb.Body{
			Payload: result,
		},
	}
	return []*isb.Message{writeMessage}, nil
}
