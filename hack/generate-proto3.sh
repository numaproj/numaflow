set -o errexit
set -o nounset
set -o pipefail

if [ "`command -v protoc`" = "" ]; then
  echo "Please install protobuf with - brew install protobuf"
  exit 1
fi

export PATH="$PATH:$(go env GOPATH)/bin"

if [ "`command -v protoc-gen-go`" = "" ]; then
  go install -mod=vendor ./vendor/google.golang.org/protobuf/cmd/protoc-gen-go
fi

if [ "`command -v protoc-gen-go-grpc`" = "" ]; then
  go install -mod=vendor ./vendor/google.golang.org/grpc/cmd/protoc-gen-go-grpc
fi

protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. -I. $(find pkg/apis/proto/isb -name '*.proto')