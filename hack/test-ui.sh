#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "Test UI"

ensure_node
ensure_yarn

test_ui() {
    yarn --cwd ui lint
    yarn --cwd ui test ${UI_TEST_MAX_WORKERS:+--maxWorkers=$UI_TEST_MAX_WORKERS}
}

test_ui