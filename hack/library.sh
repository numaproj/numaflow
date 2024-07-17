#!/bin/bash

readonly REPO_ROOT="$(git rev-parse --show-toplevel)"

# Display a box banner.
# Parameters: $1 - character to use for the box.
#             $2 - banner message.
function make_banner() {
  local msg="$1$1$1$1 $2 $1$1$1$1"
  local border="${msg//[-0-9A-Za-z _.,:\/()]/$1}"
  echo -e "${border}\n${msg}\n${border}"
}

# Simple header for logging purposes.
function header() {
  local upper="$(echo $1 | tr a-z A-Z)"
  make_banner "+" "${upper}"
}

# Simple subheader for logging purposes.
function subheader() {
  make_banner "-" "$1"
}

# Simple warning banner for logging purposes.
function warning() {
  make_banner "!" "$1"
}

function make_fake_paths() {
  FAKE_GOPATH="$(mktemp -d)"
  trap 'rm -rf ${FAKE_GOPATH}' EXIT
  FAKE_REPOPATH="${FAKE_GOPATH}/src/github.com/numaproj/numaflow"
  mkdir -p "$(dirname "${FAKE_REPOPATH}")" && ln -s "${REPO_ROOT}" "${FAKE_REPOPATH}"
}

ensure_vendor() {	
  go mod vendor
}	

ensure_node(){
  if [[ "`command -v node`" = "" ]]; then
      warning "Please install node with - brew install node"
      exit 1
  fi
}

ensure_yarn(){
  if [[ "`command -v yarn`" = "" ]]; then
      warning "Please install yarn with - brew install yarn"
      exit 1
  fi
}

uname_os() {
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  case "$os" in
    msys*) os="windows" ;;
    mingw*) os="windows" ;;
    cygwin*) os="windows" ;;
    win*) os="windows" ;;
    sunos) [ "$(uname -o)" = "illumos" ] && os=illumos ;;
  esac
  echo "$os"
}

uname_arch() {
  arch=$(uname -m)
  case $arch in
    x86_64) arch="amd64" ;;
    x86) arch="386" ;;
    i686) arch="386" ;;
    i386) arch="386" ;;
    i86pc) arch="amd64" ;;
    aarch64) arch="arm64" ;;
    armv5*) arch="armv5" ;;
    armv6*) arch="armv6" ;;
    armv7*) arch="armv7" ;;
    loongarch64) arch="loong64" ;;
  esac
  echo "${arch}"
}
