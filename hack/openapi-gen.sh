#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "updating open-apis"

ensure_vendor

CODEGEN_PKG=${REPO_ROOT}/vendor/k8s.io/kube-openapi
go run ${CODEGEN_PKG}/cmd/openapi-gen/openapi-gen.go \
    --go-header-file ${REPO_ROOT}/hack/boilerplate/boilerplate.go.txt \
    --output-dir pkg/apis/numaflow/v1alpha1 \
    --output-pkg v1alpha1 \
    --output-file zz_generated.openapi.go \
    github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1

