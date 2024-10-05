#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "running codegen"

ensure_vendor

CODEGEN_PKG=${CODEGEN_PKG:-$(cd "${REPO_ROOT}"; ls -d -1 ./vendor/k8s.io/code-generator 2>/dev/null || echo ../code-generator)}

source "${CODEGEN_PKG}/kube_codegen.sh"

THIS_PKG="github.com/numaproj/numaflow"

subheader "running deepcopy gen"

kube::codegen::gen_helpers \
    --boilerplate "${REPO_ROOT}/hack/boilerplate/boilerplate.go.txt" \
    "${REPO_ROOT}/pkg/apis"

subheader "running clients gen"

kube::codegen::gen_client \
    --with-watch \
    --output-dir "${REPO_ROOT}/pkg/client" \
    --output-pkg "${THIS_PKG}/pkg/client" \
    --boilerplate "${REPO_ROOT}/hack/boilerplate/boilerplate.go.txt" \
    --plural-exceptions "Vertex:Vertices,MonoVertex:MonoVertices" \
    --one-input-api "numaflow/v1alpha1" \
    "${REPO_ROOT}/pkg/apis"

# gofmt the tree
subheader "running gofmt"
find . -name "*.go" -type f -print0 | xargs -0 gofmt -s -w

