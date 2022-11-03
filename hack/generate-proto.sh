#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "generating proto files"

ensure_protobuf
ensure_vendor

if [ "`command -v protoc-gen-gogo`" = "" ]; then
  go install -mod=vendor ./vendor/github.com/gogo/protobuf/protoc-gen-gogo
fi

if [ "`command -v protoc-gen-gogofast`" = "" ]; then
  go install -mod=vendor ./vendor/github.com/gogo/protobuf/protoc-gen-gogofast
fi

if [ "`command -v gogoproto`" = "" ]; then
  go install -mod=vendor ./vendor/github.com/gogo/protobuf/gogoproto
fi

if [ "`command -v protoc-gen-grpc-gateway`" = "" ]; then
  go install -mod=vendor ./vendor/github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway
fi

if [ "`command -v goimports`" = "" ]; then
  export GO111MODULE="off"
  go get golang.org/x/tools/cmd/goimports
  export GO111MODULE="on"
fi

export PATH="$(go env GOPATH)/bin:${PATH}"

make_fake_paths
export GOPATH="${FAKE_GOPATH}"
cd "${FAKE_REPOPATH}"

go install -mod=vendor ./vendor/k8s.io/code-generator/cmd/go-to-protobuf

export GO111MODULE="off"

${GOPATH}/bin/go-to-protobuf \
        --go-header-file=./hack/boilerplate/boilerplate.go.txt \
        --packages=github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1 \
        --apimachinery-packages=+k8s.io/apimachinery/pkg/util/intstr,+k8s.io/apimachinery/pkg/api/resource,k8s.io/apimachinery/pkg/runtime/schema,+k8s.io/apimachinery/pkg/runtime,k8s.io/apimachinery/pkg/apis/meta/v1,k8s.io/api/core/v1,k8s.io/api/policy/v1beta1 \
        --proto-import ./vendor

# Following 2 proto files are needed
mkdir -p ${GOPATH}/src/google/api
curl -Ls https://raw.githubusercontent.com/grpc-ecosystem/grpc-gateway/v1.16.0/third_party/googleapis/google/api/annotations.proto -o ${GOPATH}/src/google/api/annotations.proto
curl -Ls https://raw.githubusercontent.com/grpc-ecosystem/grpc-gateway/v1.16.0/third_party/googleapis/google/api/http.proto -o ${GOPATH}/src/google/api/http.proto

gen-protoc(){
    protoc \
      -I /usr/local/include \
      -I . \
      -I ./vendor \
      -I ${GOPATH}/src \
      -I ./vendor/github.com/gogo/protobuf/gogoproto \
      --gogofast_out=plugins=grpc:${GOPATH}/src \
      --grpc-gateway_out=logtostderr=true:${GOPATH}/src \
      $@
}

gen-protoc pkg/apis/proto/daemon/daemon.proto
