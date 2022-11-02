#!/bin/bash
set -eu -o pipefail

source $(dirname $0)/library.sh
ensure_vendor

if [ "$(command -v controller-gen)" = "" ]; then
  go install sigs.k8s.io/controller-tools/cmd/controller-gen
fi

header "Generating CRDs"
# maxDescLen=0 avoids `kubectl apply` failing due to annotations being too long
$(go env GOPATH)/bin/controller-gen crd:crdVersions=v1,maxDescLen=0 paths=./pkg/apis/... output:dir=config/base/crds/full

cp config/base/crds/full/numaflow.numaproj.io*.yaml config/base/crds/minimal/

find config/base/crds/minimal -name 'numaflow.numaproj.io*.yaml' | while read -r file; do
  echo "Patching ${file}"
  # remove junk fields
  go run ./hack/crdgen cleancrd "$file"
done

