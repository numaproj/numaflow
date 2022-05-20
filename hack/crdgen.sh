#!/bin/bash
set -eu -o pipefail

source $(dirname $0)/library.sh
ensure_vendor

if [ "$(command -v controller-gen)" = "" ]; then
  go install sigs.k8s.io/controller-tools/cmd/controller-gen
fi

header "Generating CRDs"
$(go env GOPATH)/bin/controller-gen crd:crdVersions=v1,maxDescLen=262143 paths=./pkg/apis/... output:dir=config/base/crds

