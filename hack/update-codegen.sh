#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "running codegen"

ensure_vendor
make_fake_paths

export GOPATH="${FAKE_GOPATH}"
export GO111MODULE="off"

cd "${FAKE_REPOPATH}"

CODEGEN_PKG=${CODEGEN_PKG:-$(cd "${FAKE_REPOPATH}"; ls -d -1 ./vendor/k8s.io/code-generator 2>/dev/null || echo ../code-generator)}

subheader "running codegen"
bash -x ${CODEGEN_PKG}/generate-groups.sh "deepcopy" \
  github.com/numaproj/numaflow/pkg/client github.com/numaproj/numaflow/pkg/apis \
  "numaflow:v1alpha1" \
  --go-header-file hack/boilerplate/boilerplate.go.txt

bash -x ${CODEGEN_PKG}/generate-groups.sh "client,informer,lister" \
  github.com/numaproj/numaflow/pkg/client github.com/numaproj/numaflow/pkg/apis \
  "numaflow:v1alpha1" \
  --plural-exceptions="Vertex:Vertices" \
  --go-header-file hack/boilerplate/boilerplate.go.txt

# gofmt the tree
subheader "running gofmt"
find . -name "*.go" -type f -print0 | xargs -0 gofmt -s -w

