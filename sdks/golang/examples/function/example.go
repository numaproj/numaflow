package main

import (
	"context"

	funcsdk "github.com/numaproj/numaflow/sdks/golang/function"
)

func handle(ctx context.Context, key, msg []byte) (funcsdk.Messages, error) {
	return funcsdk.MessagesBuilder().Append(funcsdk.MessageToAll(msg)), nil
}

func main() {
	funcsdk.Start(context.Background(), handle)
}
