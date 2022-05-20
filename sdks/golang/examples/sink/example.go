package main

import (
	"context"
	"fmt"

	sinksdk "github.com/numaproj/numaflow/sdks/golang/sink"
)

func handle(ctx context.Context, msgs []sinksdk.Message) (sinksdk.Responses, error) {
	result := sinksdk.ResponsesBuilder()
	for _, m := range msgs {
		fmt.Println(string(m.Payload))
		result = result.Append(sinksdk.ResponseOK(m.ID))
	}
	return result, nil
}

func main() {
	sinksdk.Start(context.Background(), handle)
}
