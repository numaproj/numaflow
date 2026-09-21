#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

source "$(dirname "$0")/library.sh"
header "updating API v2 server interface"

readonly OAPI_CODEGEN_VERSION="v2.4.1"
readonly SPEC="${REPO_ROOT}/api/openapi-spec/numaflow-v2.yaml"

mkdir -p "${REPO_ROOT}/server/apis/v2/generated"

cd "${REPO_ROOT}"
go run "github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@${OAPI_CODEGEN_VERSION}" \
  --config api/openapi-spec/oapi-codegen-v2-server.yaml \
  "${SPEC}"
