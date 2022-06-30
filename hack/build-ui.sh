#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "Build UI"

ensure_yarn

build_ui() {
    yarn --cwd ui install
    yarn --cwd ui build
}

build_ui