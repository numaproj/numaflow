package v1

//go:generate mockgen -destination sourcemock/sourcemock.go -package sourcemock github.com/numaproj/numaflow-go/pkg/apis/proto/source/v1 SourceClient,Source_ReadFnClient
