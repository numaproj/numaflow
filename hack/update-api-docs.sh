#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source $(dirname $0)/library.sh
header "updating api docs"

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

install-pandoc() {
  # pandoc version
  PANDOC_VERSION=3.2.1

  if [[ "`command -v pandoc`" != "" ]]; then
    if [[ "`pandoc -v | head -1 | awk '{print $2}'`" != "${PANDOC_VERSION}" ]]; then
      warning "Existing pandoc version does not match the requirement (${PANDOC_VERSION}), will download a new one..."
    else
      PANDOC_BINARY="`command -v pandoc`"
      return 
    fi
  fi

  PD_REL="https://github.com/jgm/pandoc/releases"
  OS=$(uname_os)
  ARCH=$(uname_arch)

  echo "OS: $OS  ARCH: $ARCH"

  BINARY_NAME="pandoc-${PANDOC_VERSION}-${OS}-${ARCH}.zip"
  if [[ "$OS" = "darwin" ]]; then
    if [[ "$ARCH" = "arm64" ]]; then
      BINARY_NAME="pandoc-${PANDOC_VERSION}-arm64-macOS.zip"
    elif [[ "$ARCH" = "amd64" ]]; then
      BINARY_NAME="pandoc-${PANDOC_VERSION}-x86_64-macOS.zip"
    fi
  elif [[ "$OS" = "linux" ]]; then
    if [[ "$ARCH" = "arm64" ]]; then
      BINARY_NAME="pandoc-${PANDOC_VERSION}-linux-arm64.tar.gz"
    elif [[ "$ARCH" = "amd64" ]]; then
      BINARY_NAME="pandoc-${PANDOC_VERSION}-linux-amd64.tar.gz"
    fi
  fi
  BINARY_URL=$PD_REL/download/${PANDOC_VERSION}/${BINARY_NAME}
  echo "Downloading $BINARY_URL"

  tmp=$(mktemp -d)
  trap 'rm -rf ${tmp}' EXIT

  curl -sL -o ${tmp}/${BINARY_NAME} $BINARY_URL
  if [[ "$BINARY_NAME" =~ .zip$ ]]; then
    unzip ${tmp}/${BINARY_NAME} -d ${tmp}
    for a in `ls -d -1 ${tmp}/* | grep pandoc | grep -v zip`; do mv $a/* ${tmp}; rmdir $a; done
  elif [[ "$BINARY_NAME" =~ .tar.gz$ ]]; then
    tar xvzf ${tmp}/${BINARY_NAME} --strip-components 1 -C ${tmp}/
  fi
  PANDOC_BINARY="${tmp}/bin/pandoc"
}

install-pandoc

${PANDOC_BINARY} --from markdown --to gfm ${FAKE_REPOPATH}/docs/APIs.html > ${FAKE_REPOPATH}/docs/APIs.md

rm ${FAKE_REPOPATH}/docs/APIs.html

