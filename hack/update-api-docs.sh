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
  local install_dir=""
  while [[ "$#" -gt 0 ]]; do
    case "$1" in
      "--install-dir")
        install_dir="$2"
        shift 2
        ;;
      *)
        if [[ "$1" =~ ^-- ]]; then
          echo "unknown argument: $1" >&2
          return 1
        fi
        if [ -n "$install_dir" ]; then
          echo "too many arguments: $1 (already have $install_dir)" >&2
          return 1
        fi
        install_dir="$1"
        shift
        ;;
    esac
  done

  if [[ -z "${install_dir}" ]]; then
    echo "install-dir argument is required" >&2
    return 1
  fi

  if [[ ! -d "${install_dir}" ]]; then
    echo "${install_dir} does not exist" >&2
    return 1
  fi

  # pandoc version
  local pandoc_version=3.2.1
  local pd_rel="https://github.com/jgm/pandoc/releases"
  local os=$(uname_os)
  local arch=$(uname_arch)

  echo "OS: $os  ARCH: $arch"

  local binary_name="pandoc-${pandoc_version}-${os}-${arch}.zip"
  if [[ "$os" = "darwin" ]]; then
    if [[ "$arch" = "arm64" ]]; then
      binary_name="pandoc-${pandoc_version}-arm64-macOS.zip"
    elif [[ "$arch" = "amd64" ]]; then
      binary_name="pandoc-${pandoc_version}-x86_64-macOS.zip"
    fi
  elif [[ "$os" = "linux" ]]; then
    if [[ "$arch" = "arm64" ]]; then
      binary_name="pandoc-${pandoc_version}-linux-arm64.tar.gz"
    elif [[ "$arch" = "amd64" ]]; then
      binary_name="pandoc-${pandoc_version}-linux-amd64.tar.gz"
    fi
  fi
  local binary_url=$pd_rel/download/${pandoc_version}/${binary_name}
  echo "Downloading $binary_url"

  curl -sL -o ${tmp}/${binary_name} $binary_url
  if [[ "$binary_name" =~ .zip$ ]]; then
    unzip ${install_dir}/${binary_name} -d ${install_dir}
    for a in `ls -d -1 ${install_dir}/* | grep pandoc | grep -v zip`; do mv $a/* ${install_dir}; rmdir $a; done
  elif [[ "$binary_name" =~ .tar.gz$ ]]; then
    tar xvzf ${install_dir}/${binary_name} --strip-components 1 -C ${install_dir}/
  fi
}

tmp=$(mktemp -d)
install-pandoc ${tmp}
PANDOC_BINARY="${tmp}/bin/pandoc"

${PANDOC_BINARY} --from markdown --to gfm ${FAKE_REPOPATH}/docs/APIs.html > ${FAKE_REPOPATH}/docs/APIs.md

rm -rf ${tmp}
rm ${FAKE_REPOPATH}/docs/APIs.html

