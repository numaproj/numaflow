package v1

//go:generate mockgen -destination transformmock/transformmock.go -package transformermock github.com/numaproj/numaflow-go/pkg/apis/proto/sourcetransform/v1 SourceTransformClient
