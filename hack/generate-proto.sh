#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "generating proto files"

ensure_vendor

make_fake_paths
export GOPATH="${FAKE_GOPATH}"
export PATH="${GOPATH}/bin:${PATH}"
cd "${FAKE_REPOPATH}"

install-protobuf() {
  # protobuf version
  PROTOBUF_VERSION=27.2
  PB_REL="https://github.com/protocolbuffers/protobuf/releases"
  OS=$(uname_os)
  ARCH=$(uname_arch)

  echo "OS: $OS  ARCH: $ARCH"
  BINARY_URL=$PB_REL/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-${OS}-${ARCH}.zip
  if [[ "$OS" = "darwin" ]]; then
    BINARY_URL=$PB_REL/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-osx-universal_binary.zip
  elif [[ "$OS" = "linux" ]]; then
    BINARY_URL=$PB_REL/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip
  fi
  echo "Downloading $BINARY_URL"

  tmp=$(mktemp -d)
  trap 'rm -rf ${tmp}' EXIT

  curl -sL -o ${tmp}/protoc-${PROTOBUF_VERSION}-${OS}-${ARCH}.zip $BINARY_URL
  unzip ${tmp}/protoc-${PROTOBUF_VERSION}-${OS}-${ARCH}.zip -d ${GOPATH}
}

install-protobuf

go install -mod=vendor ./vendor/github.com/gogo/protobuf/protoc-gen-gogo
go install -mod=vendor ./vendor/github.com/gogo/protobuf/protoc-gen-gogofast
go install -mod=vendor ./vendor/github.com/gogo/protobuf/gogoproto
go install -mod=vendor ./vendor/google.golang.org/protobuf/cmd/protoc-gen-go
go install -mod=vendor ./vendor/google.golang.org/grpc/cmd/protoc-gen-go-grpc
go install -mod=vendor ./vendor/github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway
go install -mod=vendor ./vendor/github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2
go install -mod=vendor ./vendor/golang.org/x/tools/cmd/goimports
go install -mod=vendor ./vendor/k8s.io/code-generator/cmd/go-to-protobuf

export GO111MODULE="off"

go-to-protobuf \
        --go-header-file=./hack/boilerplate/boilerplate.go.txt \
        --packages=github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1 \
        --apimachinery-packages=+k8s.io/apimachinery/pkg/util/intstr,+k8s.io/apimachinery/pkg/api/resource,k8s.io/apimachinery/pkg/runtime/schema,+k8s.io/apimachinery/pkg/runtime,k8s.io/apimachinery/pkg/apis/meta/v1,k8s.io/api/core/v1,k8s.io/api/policy/v1beta1 \
        --proto-import ./vendor

# Following 2 proto files are needed
mkdir -p ${GOPATH}/src/google/api
curl -Ls https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto -o ${GOPATH}/src/google/api/annotations.proto
curl -Ls https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto -o ${GOPATH}/src/google/api/http.proto

gen-protoc(){
    protoc \
      -I /usr/local/include \
      -I . \
      -I ${GOPATH}/src \
      --go_out=paths=source_relative:. \
      --go-grpc_out=paths=source_relative:. \
      --grpc-gateway_out=logtostderr=true:${GOPATH}/src \
      $@
}

gen-protoc pkg/apis/proto/daemon/daemon.proto

gen-protoc pkg/apis/proto/isb/message.proto

gen-protoc pkg/apis/proto/wmb/wmb.proto

