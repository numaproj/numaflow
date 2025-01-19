#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "updating swagger"

cd ${REPO_ROOT}
mkdir -p ./dist

VERSION=$1

k8s_swagger="dist/kubernetes.swagger.json"
kubeified_swagger="dist/kubefied.swagger.json"
output="api/openapi-spec/swagger.json"

go install -mod=vendor ./vendor/github.com/go-swagger/go-swagger/cmd/swagger

curl -Ls https://raw.githubusercontent.com/kubernetes/kubernetes/release-1.31/api/openapi-spec/swagger.json -o ${k8s_swagger}

go run ./hack/gen-openapi-spec/main.go ${VERSION} ${k8s_swagger} ${kubeified_swagger}

$(go env GOPATH)/bin/swagger flatten --with-flatten minimal ${kubeified_swagger} -o ${output}

$(go env GOPATH)/bin/swagger validate ${output}
