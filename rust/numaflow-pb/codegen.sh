#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/../../hack/library.sh
header "generating pb files"

tmpath=$(mktemp -d)
trap 'rm -rf ${tmpath}' EXIT

export PATH="${tmpath}/bin:${PATH}"

install-protobuf --install-dir ${tmpath}

RUST_BACKTRACE=1 cargo run

cargo fmt
