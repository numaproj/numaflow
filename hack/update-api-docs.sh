#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "updating api docs"

ensure_pandoc
ensure_vendor
make_fake_paths

export GOPATH="${FAKE_GOPATH}"
export GO111MODULE="off"

cd "${FAKE_REPOPATH}"

# Setup - https://github.com/ahmetb/gen-crd-api-reference-docs

go run ${FAKE_REPOPATH}/vendor/github.com/ahmetb/gen-crd-api-reference-docs/main.go \
 -config "${FAKE_REPOPATH}/vendor/github.com/ahmetb/gen-crd-api-reference-docs/example-config.json" \
 -api-dir "github.com/numaproj/numaflow/pkg/apis/numaflow/v1alpha1" \
 -out-file "${FAKE_REPOPATH}/docs/APIs.html" \
 -template-dir "${FAKE_REPOPATH}/hack/api-docs-template"

# Setup - https://pandoc.org/installing.html

pandoc --from markdown --to gfm ${FAKE_REPOPATH}/docs/APIs.html > ${FAKE_REPOPATH}/docs/APIs.md

rm ${FAKE_REPOPATH}/docs/APIs.html

