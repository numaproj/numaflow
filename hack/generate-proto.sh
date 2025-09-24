#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "generating proto files"

ensure_vendor

make_fake_paths
# This overrides the existing trap
trap 'cd "${FAKE_GOPATH}" && go clean -modcache && rm -rf "${FAKE_GOPATH}"' EXIT
export GOPATH="${FAKE_GOPATH}"
export PATH="${GOPATH}/bin:${PATH}"
cd "${FAKE_REPOPATH}"

install-protobuf --install-dir ${GOPATH}

go install -mod=vendor ./vendor/github.com/gogo/protobuf/protoc-gen-gogo
go install -mod=vendor ./vendor/github.com/gogo/protobuf/protoc-gen-gogofast
go install -mod=vendor ./vendor/github.com/gogo/protobuf/gogoproto
go install -mod=vendor ./vendor/google.golang.org/protobuf/cmd/protoc-gen-go
go install -mod=vendor ./vendor/google.golang.org/grpc/cmd/protoc-gen-go-grpc
go install -mod=vendor ./vendor/github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway
go install -mod=vendor ./vendor/github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2
go install -mod=vendor ./vendor/golang.org/x/tools/cmd/goimports
go install -mod=vendor ./vendor/k8s.io/code-generator/cmd/go-to-protobuf

export GO111MODULE="on"

# go-to-protobuf expects dependency proto files to be in $GOPATH/src. Copy them there.
rm -rf "${GOPATH}/src/k8s.io/apimachinery" && mkdir -p "${GOPATH}/src/k8s.io" && cp -r "${FAKE_REPOPATH}/vendor/k8s.io/apimachinery" "${GOPATH}/src/k8s.io"
rm -rf "${GOPATH}/src/k8s.io/api" && mkdir -p "${GOPATH}/src/k8s.io" && cp -r "${FAKE_REPOPATH}/vendor/k8s.io/api" "${GOPATH}/src/k8s.io"

go-to-protobuf \
        --go-header-file=./hack/boilerplate/boilerplate.go.txt \
        --packages=github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1 \
        --apimachinery-packages=+k8s.io/apimachinery/pkg/util/intstr,+k8s.io/apimachinery/pkg/api/resource,+k8s.io/apimachinery/pkg/runtime/schema,+k8s.io/apimachinery/pkg/runtime,k8s.io/apimachinery/pkg/apis/meta/v1,k8s.io/api/core/v1,k8s.io/api/policy/v1beta1 \
        --output-dir="${GOPATH}/src/" \
        --proto-import ./vendor

# Following 2 proto files are needed
mkdir -p ${GOPATH}/src/google/api
curl -Ls https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto -o ${GOPATH}/src/google/api/annotations.proto
curl -Ls https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto -o ${GOPATH}/src/google/api/http.proto

gen-protoc(){
    protoc \
      -I /usr/local/include \
      -I . \
      -I pkg/apis/proto \
      -I ${GOPATH}/src \
      --go_out=paths=source_relative:. \
      --go-grpc_out=paths=source_relative:. \
      --grpc-gateway_out=logtostderr=true:${GOPATH}/src \
      $@
}

gen-protoc pkg/apis/proto/daemon/daemon.proto

gen-protoc pkg/apis/proto/mvtxdaemon/mvtxdaemon.proto

gen-protoc pkg/apis/proto/metadata.proto

gen-protoc pkg/apis/proto/isb/message.proto

gen-protoc pkg/apis/proto/watermark/watermark.proto

