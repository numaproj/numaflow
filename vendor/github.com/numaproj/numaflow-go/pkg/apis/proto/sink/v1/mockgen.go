package v1

//go:generate mockgen -destination sinkmock/sinkmock.go -package sinkmock github.com/numaproj/numaflow-go/pkg/apis/proto/sink/v1 SinkClient,Sink_SinkFnClient
