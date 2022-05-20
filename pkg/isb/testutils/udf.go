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
	var key []byte

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
