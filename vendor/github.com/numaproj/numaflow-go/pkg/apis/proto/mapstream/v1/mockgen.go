package v1

//go:generate mockgen -destination mapstreammock/mapstreammock.go -package mapstreammock github.com/numaproj/numaflow-go/pkg/apis/proto/mapstream/v1 MapStreamClient,MapStream_MapStreamFnClient
